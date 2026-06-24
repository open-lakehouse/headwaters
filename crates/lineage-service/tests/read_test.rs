//! End-to-end tests for the Marquez-compatible read layer: write known events to
//! a temp Delta table, then assert each derived endpoint reconstructs the
//! expected model and lineage graph.

use std::sync::Arc;

use deltalake::arrow::array::{RecordBatch, StringArray, TimestampMicrosecondArray};

use lineage_service::config::{Config, DeltaConfig};
use lineage_service::read::LineageStore;
use lineage_service::writer::delta::DeltaWriter;
use lineage_service::writer::schema::arrow_schema;

fn local_config(path: &str) -> Config {
    Config {
        delta: DeltaConfig {
            table_path: path.to_string(),
            partition_cols: vec![],
            ..DeltaConfig::default()
        },
        ..Config::default()
    }
}

/// One `run` event row: a job in `etl` reading `raw.orders` and writing
/// `marts.daily_orders`.
fn run_event_batch() -> RecordBatch {
    let schema = arrow_schema();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["run"])), // event_kind
            Arc::new(StringArray::from(vec![Some("COMPLETE")])), // event_type
            Arc::new(
                // event_time
                TimestampMicrosecondArray::from(vec![1_700_000_000_000_000i64])
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["test-producer"])), // producer
            Arc::new(StringArray::from(vec![None::<&str>])),    // schema_url
            Arc::new(StringArray::from(vec![Some("run-1")])),   // run_id
            Arc::new(StringArray::from(vec![Some("etl")])),     // job_namespace
            Arc::new(StringArray::from(vec![Some("build_daily")])), // job_name
            Arc::new(StringArray::from(vec![None::<&str>])),    // dataset_namespace
            Arc::new(StringArray::from(vec![None::<&str>])),    // dataset_name
            Arc::new(StringArray::from(vec![None::<&str>])),    // facets_json
            Arc::new(StringArray::from(vec![Some(
                // inputs_json
                r#"[{"namespace":"raw","name":"orders"}]"#,
            )])),
            Arc::new(StringArray::from(vec![Some(
                // outputs_json
                r#"[{"namespace":"marts","name":"daily_orders"}]"#,
            )])),
            Arc::new(StringArray::from(vec![None::<&str>])), // column_lineage_json
            Arc::new(StringArray::from(vec![None::<&str>])), // raw_json
        ],
    )
    .unwrap()
}

async fn seeded_store() -> (tempfile::TempDir, LineageStore) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = local_config(tmp.path().to_str().unwrap());
    DeltaWriter::new(&cfg)
        .append(run_event_batch())
        .await
        .unwrap();
    let store = LineageStore::from_config(&cfg).await.unwrap();
    (tmp, store)
}

/// Build one `run` event row. Every field threads through so tests can vary the
/// event_type, run_id, dataset namespaces, and raw_json.
#[allow(clippy::too_many_arguments)]
fn run_row(
    event_type: &str,
    event_time: i64,
    run_id: &str,
    job_ns: &str,
    job_name: &str,
    inputs_json: Option<&str>,
    outputs_json: Option<&str>,
    raw_json: Option<&str>,
) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(StringArray::from(vec!["run"])),
            Arc::new(StringArray::from(vec![Some(event_type.to_string())])),
            Arc::new(TimestampMicrosecondArray::from(vec![event_time]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["test-producer"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some(run_id.to_string())])),
            Arc::new(StringArray::from(vec![Some(job_ns.to_string())])),
            Arc::new(StringArray::from(vec![Some(job_name.to_string())])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![inputs_json.map(str::to_string)])),
            Arc::new(StringArray::from(vec![outputs_json.map(str::to_string)])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![raw_json.map(str::to_string)])),
        ],
    )
    .unwrap()
}

/// A job writing a dataset with a URI-style namespace (`s3://bucket`), seeded via
/// START (carries edges) + COMPLETE (drops them). Exercises C3 (URI nodeId) and
/// C7 (edge union + run state) together.
async fn uri_seeded_store() -> (tempfile::TempDir, LineageStore) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = local_config(tmp.path().to_str().unwrap());
    let writer = DeltaWriter::new(&cfg);
    let outputs = r#"[{"namespace":"s3://bucket","name":"warehouse/t1"}]"#;
    let raw_start = r#"{"eventType":"START","eventTime":"2023-11-14T22:13:20Z","run":{"runId":"r1","facets":{"nominalTime":{"x":1}}},"job":{"namespace":"etl","name":"export"},"producer":"p"}"#;
    writer
        .append(run_row(
            "START",
            1_700_000_000_000_000,
            "r1",
            "etl",
            "export",
            None,
            Some(outputs),
            Some(raw_start),
        ))
        .await
        .unwrap();
    // COMPLETE with no datasets — must NOT erase the edge from the START.
    writer
        .append(run_row(
            "COMPLETE",
            1_700_000_005_000_000,
            "r1",
            "etl",
            "export",
            None,
            None,
            Some(r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:25Z","run":{"runId":"r1"},"job":{"namespace":"etl","name":"export"},"producer":"p"}"#),
        ))
        .await
        .unwrap();
    let store = LineageStore::from_config(&cfg).await.unwrap();
    (tmp, store)
}

#[tokio::test]
async fn namespaces_lists_job_and_dataset_namespaces() {
    let (_tmp, store) = seeded_store().await;
    let ns = store.namespaces().await.unwrap();
    let names: Vec<&str> = ns.namespaces.iter().map(|n| n.name.as_str()).collect();
    assert!(names.contains(&"etl"), "job namespace present: {names:?}");
    assert!(
        names.contains(&"raw"),
        "input ds namespace present: {names:?}"
    );
    assert!(
        names.contains(&"marts"),
        "output ds namespace present: {names:?}"
    );
}

#[tokio::test]
async fn jobs_returns_job_with_inputs_and_outputs() {
    let (_tmp, store) = seeded_store().await;
    let jobs = store.jobs(Some("etl"), 100, 0).await.unwrap();
    assert_eq!(jobs.total_count, 1);
    let job = &jobs.jobs[0];
    assert_eq!(job.name, "build_daily");
    assert_eq!(job.inputs.len(), 1);
    assert_eq!(job.inputs[0].namespace, "raw");
    assert_eq!(job.inputs[0].name, "orders");
    assert_eq!(job.outputs[0].name, "daily_orders");
    // One reconstructed run from the COMPLETE event, with its real state.
    assert_eq!(job.latest_runs.len(), 1);
    assert_eq!(job.latest_runs[0].id, "run-1");
    assert_eq!(job.latest_runs[0].state, "COMPLETED");
}

#[tokio::test]
async fn datasets_include_job_referenced_tables() {
    let (_tmp, store) = seeded_store().await;
    let raw = store.datasets(Some("raw"), 100, 0).await.unwrap();
    assert_eq!(raw.datasets.len(), 1);
    assert_eq!(raw.datasets[0].name, "orders");

    let marts = store.datasets(Some("marts"), 100, 0).await.unwrap();
    assert_eq!(marts.datasets[0].name, "daily_orders");
}

#[tokio::test]
async fn lineage_graph_connects_job_to_its_datasets() {
    let (_tmp, store) = seeded_store().await;
    let node = "job:etl:build_daily";
    let graph = store.lineage(node, 2).await.unwrap();

    // Seed job + one input + one output dataset reachable within 2 hops.
    let ids: Vec<&str> = graph.graph.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&node), "seed job present: {ids:?}");
    assert!(
        ids.contains(&"dataset:raw:orders"),
        "input present: {ids:?}"
    );
    assert!(
        ids.contains(&"dataset:marts:daily_orders"),
        "output present: {ids:?}"
    );

    // The job node should have one in-edge (from input) and one out-edge (to output).
    let job = graph.graph.iter().find(|n| n.id == node).unwrap();
    assert_eq!(job.node_type, "JOB");
    assert_eq!(job.in_edges.len(), 1);
    assert_eq!(job.in_edges[0].origin, "dataset:raw:orders");
    assert_eq!(job.out_edges.len(), 1);
    assert_eq!(job.out_edges[0].destination, "dataset:marts:daily_orders");
}

#[tokio::test]
async fn search_matches_job_and_dataset_names() {
    let (_tmp, store) = seeded_store().await;
    let hits = store.search("orders", 100).await.unwrap();
    // Matches both "raw.orders" and "marts.daily_orders".
    assert!(hits.total_count >= 2, "got {} hits", hits.total_count);
    assert!(hits.results.iter().all(|r| r.result_type == "DATASET"));
}

#[tokio::test]
async fn empty_table_yields_empty_results() {
    // Point at a path with no table — should be empty, not an error.
    let tmp = tempfile::tempdir().unwrap();
    let cfg = local_config(tmp.path().join("nonexistent").to_str().unwrap());
    let store = LineageStore::from_config(&cfg).await.unwrap();
    let ns = store.namespaces().await.unwrap();
    assert!(ns.namespaces.is_empty());
}

#[tokio::test]
async fn missing_job_is_not_found() {
    let (_tmp, store) = seeded_store().await;
    let err = store.job("etl", "nope").await.unwrap_err();
    assert!(matches!(err, lineage_service::read::ReadError::NotFound(_)));
}

// --- HTTP-level tests ---------------------------------------------------------
// These drive the read router the way the Marquez UI does, so they catch issues
// the direct store-call tests can't: route-pattern syntax (axum 0.7 uses
// `:param`, not `{param}`) and the camelCase JSON envelope keys.

use http_body_util::BodyExt;
use lineage_service::read::http::router as read_router;
use tower::ServiceExt; // for `oneshot`

async fn get(store: LineageStore, uri: &str) -> (axum::http::StatusCode, String) {
    let req = axum::http::Request::builder()
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = read_router(store).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn http_namespaced_jobs_route_resolves() {
    // Regression guard for the axum-0.7 `:param` route syntax: a `{param}` route
    // would 404 here even though the namespace has jobs.
    let (_tmp, store) = seeded_store().await;
    let (status, body) = get(store, "/api/v1/namespaces/etl/jobs?limit=25&offset=0").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("build_daily"), "body: {body}");
    // Envelope key must be camelCase for the Marquez UI.
    assert!(body.contains("\"totalCount\""), "body: {body}");
}

#[tokio::test]
async fn http_global_jobs_route_resolves() {
    let (_tmp, store) = seeded_store().await;
    let (status, body) = get(store, "/api/v1/jobs?limit=25&offset=0").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("build_daily"), "body: {body}");
}

#[tokio::test]
async fn http_job_runs_returns_real_run_state() {
    let (_tmp, store) = seeded_store().await;
    let (status, body) = get(
        store,
        "/api/v1/namespaces/etl/jobs/build_daily/runs?limit=14&offset=0",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"runs\""), "body: {body}");
    assert!(body.contains("\"totalCount\""), "body: {body}");
    assert!(body.contains("COMPLETED"), "body: {body}");
    assert!(body.contains("run-1"), "real runId surfaced: {body}");
}

#[tokio::test]
async fn http_lineage_node_edges_are_camel_case() {
    // The UI's graph layout reads node.inEdges / node.outEdges; snake_case keys
    // make those `undefined` and crash `findDownstreamNodes`.
    let (_tmp, store) = seeded_store().await;
    let (status, body) = get(store, "/api/v1/lineage?nodeId=job:etl:build_daily&depth=2").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"outEdges\""), "body: {body}");
    assert!(body.contains("\"inEdges\""), "body: {body}");
    assert!(!body.contains("\"out_edges\""), "snake_case leaked: {body}");
}

#[tokio::test]
async fn http_search_envelope_is_camel_case() {
    let (_tmp, store) = seeded_store().await;
    let (status, body) = get(store, "/api/v1/search?q=orders&limit=100").await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(body.contains("\"totalCount\""), "body: {body}");
    assert!(body.contains("\"nodeId\""), "body: {body}");
}

// --- C3: URI-namespace nodeIds round-trip through lineage ---------------------

#[tokio::test]
async fn lineage_resolves_uri_namespace_dataset() {
    // C3 regression: dataset:s3://bucket:warehouse/t1 must resolve to the real
    // dataset (namespace s3://bucket), not a synthetic node from a mangled parse.
    let (_tmp, store) = uri_seeded_store().await;
    let node = "dataset:s3://bucket:warehouse/t1";
    let graph = store.lineage(node, 2).await.unwrap();
    let ids: Vec<&str> = graph.graph.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&node), "uri dataset present: {ids:?}");
    assert!(
        ids.contains(&"job:etl:export"),
        "connected job present: {ids:?}"
    );
    // The dataset node must carry its real timestamps, not the 1970 epoch a
    // synthetic empty payload would have.
    let ds = graph.graph.iter().find(|n| n.id == node).unwrap();
    assert_eq!(ds.node_type, "DATASET");
    let updated = ds.data.get("updatedAt").and_then(|v| v.as_str()).unwrap();
    assert!(!updated.starts_with("1970"), "real timestamp: {updated}");
}

// --- C7.1: edge union (START carries edges, COMPLETE drops them) --------------

#[tokio::test]
async fn complete_without_datasets_does_not_erase_edges() {
    let (_tmp, store) = uri_seeded_store().await;
    let job = store.job("etl", "export").await.unwrap();
    assert_eq!(job.outputs.len(), 1, "START's output survives the COMPLETE");
    assert_eq!(job.outputs[0].namespace, "s3://bucket");
    assert_eq!(job.outputs[0].name, "warehouse/t1");
}

// --- C7.2: real run state from START->COMPLETE --------------------------------

#[tokio::test]
async fn run_state_reflects_terminal_event() {
    let (_tmp, store) = uri_seeded_store().await;
    let runs = store.job_runs("etl", "export").await.unwrap();
    assert_eq!(runs.total_count, 1);
    assert_eq!(runs.runs[0].id, "r1");
    assert_eq!(runs.runs[0].state, "COMPLETED");
    // START(1_700_000_000)->COMPLETE(1_700_000_005) = 5s.
    assert_eq!(runs.runs[0].duration_ms, 5_000);
}

// --- C9.1: unknown lineage seed -> 404 ----------------------------------------

#[tokio::test]
async fn lineage_unknown_seed_is_not_found() {
    let (_tmp, store) = seeded_store().await;
    let err = store.lineage("dataset:nope:missing", 2).await.unwrap_err();
    assert!(
        matches!(err, lineage_service::read::ReadError::NotFound(_)),
        "unknown seed should 404, got {err:?}"
    );
}

// --- C8: the four endpoints marquez-web calls ---------------------------------

#[tokio::test]
async fn http_events_lineage_returns_raw_events() {
    let (_tmp, store) = uri_seeded_store().await;
    let (status, body) = get(store, "/api/v1/events/lineage?limit=10&offset=0").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"events\""), "envelope: {body}");
    assert!(body.contains("\"totalCount\""), "envelope: {body}");
    // The raw OpenLineage eventType must come through for the UI's badge.
    assert!(
        body.contains("START") || body.contains("COMPLETE"),
        "body: {body}"
    );
}

#[tokio::test]
async fn http_dataset_versions_returns_a_version() {
    let (_tmp, store) = seeded_store().await;
    let (status, body) = get(
        store,
        "/api/v1/namespaces/raw/datasets/orders/versions?limit=10&offset=0",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"versions\""), "envelope: {body}");
    assert!(body.contains("\"totalCount\""), "envelope: {body}");
    assert!(body.contains("\"version\""), "version id: {body}");
}

#[tokio::test]
async fn http_dataset_versions_unknown_is_404() {
    let (_tmp, store) = seeded_store().await;
    let (status, _body) = get(
        store,
        "/api/v1/namespaces/raw/datasets/nope/versions?limit=10&offset=0",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_run_facets_returns_run_facets() {
    let (_tmp, store) = uri_seeded_store().await;
    let (status, body) = get(store, "/api/v1/jobs/runs/r1/facets").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"runId\""), "envelope: {body}");
    assert!(body.contains("\"facets\""), "envelope: {body}");
    // The nominalTime facet from the START event's raw_json should be merged in.
    assert!(body.contains("nominalTime"), "facet merged: {body}");
}

#[tokio::test]
async fn http_run_facets_unknown_run_is_404() {
    let (_tmp, store) = seeded_store().await;
    let (status, _body) = get(store, "/api/v1/jobs/runs/no-such-run/facets").await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_column_lineage_returns_empty_graph_not_404() {
    // A dataset with no stored column lineage renders an empty column view.
    let (_tmp, store) = seeded_store().await;
    let (status, body) = get(store, "/api/v1/column-lineage?nodeId=dataset:raw:orders").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"graph\""), "envelope: {body}");
}

/// One row with a populated `column_lineage_json`, the shape the writer
/// persists (`run_column_lineage_to_json`): per-dataset entries wrapping the
/// OpenLineage `columnLineage` facet.
fn column_lineage_row(event_time: i64, run_id: &str, column_lineage: &str) -> RecordBatch {
    RecordBatch::try_new(
        arrow_schema(),
        vec![
            Arc::new(StringArray::from(vec!["run"])),
            Arc::new(StringArray::from(vec![Some("COMPLETE")])),
            Arc::new(TimestampMicrosecondArray::from(vec![event_time]).with_timezone("UTC")),
            Arc::new(StringArray::from(vec!["test-producer"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some(run_id.to_string())])),
            Arc::new(StringArray::from(vec![Some("etl")])),
            Arc::new(StringArray::from(vec![Some("build_silver")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some(
                r#"[{"namespace":"raw","name":"customers"}]"#.to_string(),
            )])),
            Arc::new(StringArray::from(vec![Some(
                r#"[{"namespace":"warehouse","name":"silver.customers"}]"#.to_string(),
            )])),
            Arc::new(StringArray::from(vec![Some(column_lineage.to_string())])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .unwrap()
}

/// Two runs writing `warehouse:silver.customers` with column lineage; the
/// newer one maps `id` to `raw:customers.customer_key` (not `.id`), proving
/// the latest facet wins.
async fn column_lineage_seeded_store() -> (tempfile::TempDir, LineageStore) {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = local_config(tmp.path().to_str().unwrap());
    let writer = DeltaWriter::new(&cfg);
    let older = r#"{"outputs":[{"namespace":"warehouse","name":"silver.customers","columnLineage":{"fields":{"id":{"inputFields":[{"namespace":"raw","name":"customers","field":"id","transformations":[{"type":"DIRECT","subtype":"IDENTITY","description":"","masking":false}]}]}}}}]}"#;
    let newer = r#"{"outputs":[{"namespace":"warehouse","name":"silver.customers","columnLineage":{"fields":{"id":{"inputFields":[{"namespace":"raw","name":"customers","field":"customer_key","transformations":[{"type":"DIRECT","subtype":"IDENTITY","description":"","masking":false}]}]},"email_hash":{"inputFields":[{"namespace":"raw","name":"customers","field":"email","transformations":[{"type":"DIRECT","subtype":"TRANSFORMATION","description":"","masking":false}]}]}}}}]}"#;
    writer
        .append(column_lineage_row(1_700_000_000_000_000, "r1", older))
        .await
        .unwrap();
    writer
        .append(column_lineage_row(1_700_000_005_000_000, "r2", newer))
        .await
        .unwrap();
    let store = LineageStore::from_config(&cfg).await.unwrap();
    (tmp, store)
}

#[tokio::test]
async fn column_lineage_serves_latest_facet_as_field_graph() {
    let (_tmp, store) = column_lineage_seeded_store().await;
    let graph = store
        .column_lineage("dataset:warehouse:silver.customers")
        .await
        .unwrap()
        .graph;

    let ids: Vec<&str> = graph.iter().map(|n| n.id.as_str()).collect();
    assert!(
        ids.contains(&"datasetField:warehouse:silver.customers:id")
            && ids.contains(&"datasetField:warehouse:silver.customers:email_hash")
            && ids.contains(&"datasetField:raw:customers:customer_key")
            && ids.contains(&"datasetField:raw:customers:email"),
        "output + input field nodes present: {ids:?}"
    );
    assert!(
        !ids.contains(&"datasetField:raw:customers:id"),
        "the older facet's mapping must not leak in: {ids:?}"
    );
    assert!(graph.iter().all(|n| n.node_type == "DATASET_FIELD"));

    let id_node = graph
        .iter()
        .find(|n| n.id == "datasetField:warehouse:silver.customers:id")
        .unwrap();
    assert_eq!(
        id_node.in_edges[0].origin, "datasetField:raw:customers:customer_key",
        "edge mirrors the latest inputFields"
    );
    let input_node = graph
        .iter()
        .find(|n| n.id == "datasetField:raw:customers:customer_key")
        .unwrap();
    assert_eq!(
        input_node.out_edges[0].destination,
        "datasetField:warehouse:silver.customers:id"
    );
}

#[tokio::test]
async fn column_lineage_dataset_field_node_id_filters_to_one_field() {
    let (_tmp, store) = column_lineage_seeded_store().await;
    let graph = store
        .column_lineage("datasetField:warehouse:silver.customers:email_hash")
        .await
        .unwrap()
        .graph;
    let ids: Vec<&str> = graph.iter().map(|n| n.id.as_str()).collect();
    assert!(
        ids.contains(&"datasetField:warehouse:silver.customers:email_hash")
            && ids.contains(&"datasetField:raw:customers:email"),
        "addressed field + its inputs: {ids:?}"
    );
    assert!(
        !ids.iter()
            .any(|id| id.ends_with(":id") || id.ends_with(":customer_key")),
        "other fields filtered out: {ids:?}"
    );
}

#[tokio::test]
async fn http_column_lineage_serves_stored_facet() {
    let (_tmp, store) = column_lineage_seeded_store().await;
    let (status, body) = get(
        store,
        "/api/v1/column-lineage?nodeId=dataset:warehouse:silver.customers&depth=20&withDownstream=false",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("DATASET_FIELD"), "body: {body}");
    // camelCase envelope for the UI's graph layout.
    assert!(body.contains("\"inEdges\""), "body: {body}");
    assert!(body.contains("\"inputFields\""), "body: {body}");
}

// --- C9.2: search totalCount counts all matches, not the page -----------------

#[tokio::test]
async fn search_total_count_is_full_match_count() {
    let (_tmp, store) = seeded_store().await;
    // Both "raw.orders" and "marts.daily_orders" match "orders"; limit 1 returns
    // one result but totalCount must still report both.
    let hits = store.search("orders", 1).await.unwrap();
    assert_eq!(hits.results.len(), 1, "page is truncated to limit");
    assert!(
        hits.total_count >= 2,
        "totalCount counts all matches: {}",
        hits.total_count
    );
}
