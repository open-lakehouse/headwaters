//! Differential conformance harness against the OpenLineage reference
//! implementation, **Marquez**.
//!
//! This is the strongest compatibility signal the service has: it does not
//! assert against our own model of the spec, it posts the *same* OpenLineage
//! events to both a real Marquez (+ its Postgres) and our service (+ its
//! Postgres), then reads each back through their respective REST APIs and
//! asserts the lineage they reconstruct is equivalent.
//!
//! "Equivalent" is defined on a **semantic subset**: Marquez populates fields we
//! deliberately don't drive identically (server-assigned UUIDs, version hashes,
//! ingest timestamps, source rows), so a byte diff would fail on noise. We
//! normalize both responses onto the meaningful shape — dataset/job identity,
//! schema fields, run state, the lineage graph's nodes + edges, the
//! column-lineage field mappings, and selected run facets — and compare that.
//!
//! Gated behind `conformance-it` and needs Docker. Run with:
//!   cargo test -p headwaters --features conformance-it --test conformance_test
#![cfg(feature = "conformance-it")]

use std::collections::BTreeSet;
use std::time::Duration;

use headwaters::ingest::convert_event;
use headwaters::projection::project_all;
use headwaters::read::LineageStore;
use headwaters::writer::postgres::PostgresSink;
use headwaters::writer::row::event_to_row;
use headwaters::writer::sink::EventSink;
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use url::Url;

const MARQUEZ_IMAGE: &str = "marquezproject/marquez";
const MARQUEZ_TAG: &str = "0.50.0";
const POSTGRES_TAG: &str = "16";
const MARQUEZ_DB: &str = "marquez";
const NAMESPACE: &str = "conformance";

// ---------------------------------------------------------------------------
// The shared event sequence posted to BOTH backends.
// ---------------------------------------------------------------------------

/// A START + COMPLETE for a job `build_silver` in `conformance` that reads
/// `raw.customers` and writes `warehouse.silver_customers`, carrying a schema
/// facet on the output, a column-lineage facet, and a nominalTime run facet.
/// The COMPLETE drops the datasets (the common producer pattern) to also
/// exercise edge-union.
fn events() -> Vec<Value> {
    let output_with_facets = json!({
        "namespace": "warehouse",
        "name": "silver_customers",
        "facets": {
            "schema": {
                "_producer": "conformance", "_schemaURL": "x",
                "fields": [
                    {"name": "id", "type": "BIGINT"},
                    {"name": "email_hash", "type": "STRING"}
                ]
            },
            "columnLineage": {
                "_producer": "conformance", "_schemaURL": "x",
                "fields": {
                    "id": {"inputFields": [
                        {"namespace": "raw", "name": "customers", "field": "id"}
                    ]},
                    "email_hash": {"inputFields": [
                        {"namespace": "raw", "name": "customers", "field": "email"}
                    ]}
                }
            }
        }
    });
    vec![
        json!({
            "eventType": "START",
            "eventTime": "2023-11-14T22:13:20Z",
            "producer": "conformance",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "run": {
                "runId": "01000000-0000-7000-8000-000000000001",
                "facets": {
                    "nominalTime": {
                        "_producer": "conformance", "_schemaURL": "x",
                        "nominalStartTime": "2023-11-14T22:00:00Z",
                        "nominalEndTime": "2023-11-14T23:00:00Z"
                    }
                }
            },
            "job": {"namespace": NAMESPACE, "name": "build_silver"},
            "inputs": [{
                "namespace": "raw", "name": "customers",
                "facets": {
                    "schema": {
                        "_producer": "conformance", "_schemaURL": "x",
                        "fields": [
                            {"name": "id", "type": "BIGINT"},
                            {"name": "email", "type": "STRING"}
                        ]
                    }
                }
            }],
            "outputs": [output_with_facets],
        }),
        json!({
            "eventType": "COMPLETE",
            "eventTime": "2023-11-14T22:13:25Z",
            "producer": "conformance",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
            "run": {"runId": "01000000-0000-7000-8000-000000000001"},
            "job": {"namespace": NAMESPACE, "name": "build_silver"},
        }),
    ]
}

// ---------------------------------------------------------------------------
// Backend bring-up.
// ---------------------------------------------------------------------------

/// Marquez + its Postgres on a shared network; returns the host-mapped API url.
/// The containers must be kept alive for the test's duration.
async fn start_marquez() -> (
    ContainerAsync<GenericImage>,
    ContainerAsync<GenericImage>,
    Url,
) {
    let pid = std::process::id();
    let network = format!("ol-conf-{pid}");
    let pg_name = format!("ol-conf-pg-{pid}");

    let postgres = GenericImage::new("postgres", POSTGRES_TAG)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_network(network.clone())
        .with_container_name(pg_name.clone())
        .with_env_var("POSTGRES_USER", MARQUEZ_DB)
        .with_env_var("POSTGRES_PASSWORD", MARQUEZ_DB)
        .with_env_var("POSTGRES_DB", MARQUEZ_DB)
        .start()
        .await
        .expect("marquez postgres started");

    let marquez = GenericImage::new(MARQUEZ_IMAGE, MARQUEZ_TAG)
        .with_exposed_port(5000.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/healthcheck")
                .with_port(5001.tcp())
                .with_expected_status_code(200u16),
        ))
        .with_network(network)
        .with_env_var("MARQUEZ_PORT", "5000")
        .with_env_var("MARQUEZ_ADMIN_PORT", "5001")
        .with_env_var("POSTGRES_HOST", pg_name)
        .with_env_var("POSTGRES_PORT", "5432")
        .with_env_var("SEARCH_ENABLED", "false")
        .start()
        .await
        .expect("marquez started");

    let host = marquez.get_host().await.expect("marquez host");
    let port = marquez
        .get_host_port_ipv4(5000.tcp())
        .await
        .expect("marquez api port");
    let base = Url::parse(&format!("http://{host}:{port}/")).expect("base url");
    (postgres, marquez, base)
}

/// Our service's Postgres; returns a connected, migrated pool.
async fn start_our_postgres() -> (ContainerAsync<GenericImage>, PgPool) {
    let container = GenericImage::new("postgres", POSTGRES_TAG)
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "lineage")
        .start()
        .await
        .expect("our postgres started");
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/lineage");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect our postgres");
    sqlx::migrate!().run(&pool).await.expect("migrate");
    (container, pool)
}

/// POST one event to Marquez's `/api/v1/lineage`.
async fn post_marquez(http: &reqwest::Client, base: &Url, event: &Value) {
    let url = base.join("api/v1/lineage").unwrap();
    let resp = http
        .post(url)
        .json(event)
        .send()
        .await
        .expect("post marquez");
    assert!(
        resp.status().is_success(),
        "marquez rejected event: {}",
        resp.status()
    );
}

/// Ingest one event into our service (converter -> sink -> projection).
async fn ingest_ours(pool: &PgPool, event: &Value) {
    let json = serde_json::to_vec(event).unwrap();
    let owned = convert_event(&json).expect("convert");
    let row = event_to_row(owned.reborrow()).expect("row");
    PostgresSink::new(pool.clone())
        .append(&[row])
        .await
        .expect("append");
    project_all(pool).await.expect("project");
}

/// GET `base + path` from Marquez, retrying until `predicate` holds (Marquez
/// ingests asynchronously).
async fn marquez_get(
    http: &reqwest::Client,
    base: &Url,
    path: &str,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    let url = base.join(path).expect("join");
    let mut last = Value::Null;
    for _ in 0..40 {
        if let Ok(resp) = http.get(url.clone()).send().await
            && resp.status().is_success()
            && let Ok(json) = resp.json::<Value>().await
        {
            if predicate(&json) {
                return json;
            }
            last = json;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("condition never met for GET {path}; last:\n{last:#}");
}

fn enc(node_id: &str) -> String {
    node_id.replace(':', "%3A").replace('/', "%2F")
}

// ---------------------------------------------------------------------------
// Normalization: project a backend's responses onto the semantic subset.
// ---------------------------------------------------------------------------

/// The lineage graph reduced to its node ids and directed edges — the shape
/// both backends must agree on.
#[derive(Debug, PartialEq, Eq)]
struct GraphShape {
    nodes: BTreeSet<String>,
    edges: BTreeSet<(String, String)>,
}

fn graph_shape(graph: &Value) -> GraphShape {
    let mut nodes = BTreeSet::new();
    let mut edges = BTreeSet::new();
    for node in graph["graph"].as_array().into_iter().flatten() {
        let id = node["id"].as_str().unwrap_or_default().to_string();
        nodes.insert(id.clone());
        for e in node["outEdges"].as_array().into_iter().flatten() {
            if let (Some(o), Some(d)) = (e["origin"].as_str(), e["destination"].as_str()) {
                edges.insert((o.to_string(), d.to_string()));
            }
        }
        for e in node["inEdges"].as_array().into_iter().flatten() {
            if let (Some(o), Some(d)) = (e["origin"].as_str(), e["destination"].as_str()) {
                edges.insert((o.to_string(), d.to_string()));
            }
        }
    }
    GraphShape { nodes, edges }
}

/// The set of column-dependency pairings in a column-lineage graph, addressed
/// by the `datasetField:ns:name:field` node ids both backends emit.
///
/// Compared **orientation-insensitively** (each pair is sorted): the two
/// backends agree on *which* output field depends on *which* input field, but
/// disagree on the edge's `origin`/`destination` convention — Marquez orients
/// column-lineage edges output→input, while our graph (and Marquez's
/// *table-level* graph) orient input→output. The semantically meaningful
/// content is the unordered dependency pairing, so we normalize direction away.
/// We read `inEdges`; each edge is restated there on the node it terminates at.
fn column_edges(graph: &Value) -> BTreeSet<(String, String)> {
    let mut edges = BTreeSet::new();
    for node in graph["graph"].as_array().into_iter().flatten() {
        for e in node["inEdges"].as_array().into_iter().flatten() {
            if let (Some(o), Some(d)) = (e["origin"].as_str(), e["destination"].as_str()) {
                let (a, b) = (o.to_string(), d.to_string());
                edges.insert(if a <= b { (a, b) } else { (b, a) });
            }
        }
    }
    edges
}

/// Sorted schema field names of a dataset response.
fn field_names(dataset: &Value) -> Vec<String> {
    let mut names: Vec<String> = dataset["fields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|f| f["name"].as_str().map(str::to_string))
        .collect();
    names.sort();
    names
}

// ---------------------------------------------------------------------------
// The test.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn our_lineage_matches_marquez() {
    let (_m_pg, _marquez, base) = start_marquez().await;
    let (_our_pg, pool) = start_our_postgres().await;
    let store = LineageStore::new(pool.clone());
    let http = reqwest::Client::new();

    // Same events into both backends.
    for ev in events() {
        post_marquez(&http, &base, &ev).await;
        ingest_ours(&pool, &ev).await;
    }

    // === 1. Run state + job/dataset model ===================================
    // Marquez: the job carries the run; our store likewise.
    let m_job = marquez_get(
        &http,
        &base,
        &format!("api/v1/namespaces/{NAMESPACE}/jobs/build_silver"),
        |j| j["name"].as_str() == Some("build_silver"),
    )
    .await;
    let o_job = store.job(NAMESPACE, "build_silver").await.unwrap();
    let o_job = serde_json::to_value(&o_job).unwrap();

    // Inputs / outputs (by dataset name) agree.
    let names = |v: &Value, key: &str| -> Vec<String> {
        let mut n: Vec<String> = v[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| d["name"].as_str().map(str::to_string))
            .collect();
        n.sort();
        n
    };
    assert_eq!(
        names(&m_job, "inputs"),
        names(&o_job, "inputs"),
        "job inputs match"
    );
    assert_eq!(
        names(&m_job, "outputs"),
        names(&o_job, "outputs"),
        "job outputs match"
    );

    // Latest run state agrees (COMPLETED).
    let m_state = m_job["latestRun"]["state"].as_str();
    let o_state = o_job["latestRun"]["state"].as_str();
    assert_eq!(m_state, Some("COMPLETED"), "marquez run COMPLETED");
    assert_eq!(o_state, m_state, "run state matches marquez");

    // === 2. Dataset schema fields ===========================================
    let m_ds = marquez_get(
        &http,
        &base,
        "api/v1/namespaces/warehouse/datasets/silver_customers",
        |j| j["name"].as_str() == Some("silver_customers"),
    )
    .await;
    let o_ds = serde_json::to_value(
        store
            .dataset("warehouse", "silver_customers")
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        field_names(&m_ds),
        field_names(&o_ds),
        "dataset schema fields match marquez: {m_ds:#}"
    );
    assert_eq!(
        field_names(&o_ds),
        vec!["email_hash".to_string(), "id".to_string()],
        "expected fields present"
    );

    // === 3. Table-level lineage graph =======================================
    let node = "dataset:warehouse:silver_customers".to_string();
    let m_graph = marquez_get(
        &http,
        &base,
        &format!("api/v1/lineage?nodeId={}", enc(&node)),
        |j| !j["graph"].as_array().map(|a| a.is_empty()).unwrap_or(true),
    )
    .await;
    let o_graph = serde_json::to_value(store.lineage(&node, 20).await.unwrap()).unwrap();

    let m_shape = graph_shape(&m_graph);
    let o_shape = graph_shape(&o_graph);
    // The job and both datasets are nodes; edges raw.customers->job->silver.
    assert_eq!(
        o_shape.nodes, m_shape.nodes,
        "graph nodes match marquez\nmarquez: {m_shape:?}\nours: {o_shape:?}"
    );
    assert_eq!(
        o_shape.edges, m_shape.edges,
        "graph edges match marquez\nmarquez: {m_shape:?}\nours: {o_shape:?}"
    );

    // === 4. Column-level lineage ============================================
    let m_cols = marquez_get(
        &http,
        &base,
        &format!("api/v1/column-lineage?nodeId={}", enc(&node)),
        |j| !j["graph"].as_array().map(|a| a.is_empty()).unwrap_or(true),
    )
    .await;
    let o_cols = serde_json::to_value(store.column_lineage(&node).await.unwrap()).unwrap();
    assert_eq!(
        column_edges(&o_cols),
        column_edges(&m_cols),
        "column-lineage field edges match marquez\nmarquez: {m_cols:#}\nours: {o_cols:#}"
    );

    // === 5. Run facet round-trip (nominalTime) ==============================
    // The strongest facet-compatibility claim: a facet we emit is preserved
    // verbatim by *both* backends. Both expose the original `nominalTime` facet
    // under `run.facets.nominalTime` — Marquez on the run object, us on the
    // `/facets` blob — with the values we sent. (Marquez additionally tries to
    // hoist nominalTime to top-level `run.nominalStartTime`, but only for a
    // facet `_schemaURL` it recognizes; that hoisting is Marquez-internal and
    // not a compatibility surface, so we compare the preserved facet instead.)
    let run_id = "01000000-0000-7000-8000-000000000001";

    let m_run = marquez_get(&http, &base, &format!("api/v1/jobs/runs/{run_id}"), |j| {
        j["facets"]["nominalTime"].is_object()
    })
    .await;
    let m_nt = &m_run["facets"]["nominalTime"];

    let o_facets = serde_json::to_value(store.run_facets(run_id).await.unwrap()).unwrap();
    let o_nt = &o_facets["facets"]["nominalTime"];
    assert!(
        o_nt.is_object(),
        "we surface the nominalTime facet: {o_facets:#}"
    );

    // The nominal window agrees, compared as instants (ISO-8601 zone spelling
    // may differ, e.g. `Z` vs `+00:00`).
    let as_instant = |s: &Value| {
        s.as_str()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|d| d.to_utc())
    };
    assert_eq!(
        as_instant(&o_nt["nominalStartTime"]),
        as_instant(&m_nt["nominalStartTime"]),
        "nominalStartTime facet matches marquez\nours: {o_nt:#}\nmarquez: {m_nt:#}"
    );
    assert_eq!(
        as_instant(&o_nt["nominalEndTime"]),
        as_instant(&m_nt["nominalEndTime"]),
        "nominalEndTime facet matches marquez"
    );
}
