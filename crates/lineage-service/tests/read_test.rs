//! End-to-end tests for the Marquez-compatible read layer, Postgres-backed.
//!
//! Each test spins up a throwaway Postgres (testcontainers), ingests known
//! OpenLineage events through the real converter + [`PostgresSink`], folds them
//! with the projection worker, then asserts each derived endpoint reconstructs
//! the expected model and lineage graph. This is the parity harness for the
//! pre-Postgres Delta reader: the assertions are unchanged; only the storage
//! underneath them moved.
//!
//! Needs Docker — gated behind the `postgres-it` feature so the default test
//! run doesn't require a container runtime. Run with:
//!   cargo test -p lineage-service --features postgres-it --test read_test
#![cfg(feature = "postgres-it")]

use lineage_service::ingest::convert_event;
use lineage_service::projection::project_all;
use lineage_service::read::LineageStore;
use lineage_service::writer::postgres::PostgresSink;
use lineage_service::writer::row::event_to_row;
use lineage_service::writer::sink::EventSink;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// A running Postgres + a connected, migrated pool. Holds the container so it
/// outlives the test.
struct Db {
    _container: ContainerAsync<GenericImage>,
    pool: PgPool,
}

async fn start_postgres() -> Db {
    let container = GenericImage::new("postgres", "16-alpine")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "lineage")
        .start()
        .await
        .expect("start postgres");
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/lineage");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .expect("connect postgres");
    sqlx::migrate!().run(&pool).await.expect("run migrations");
    Db {
        _container: container,
        pool,
    }
}

/// Ingest one OpenLineage JSON document the way the HTTP handler does, then fold
/// the projection so the read tables reflect it.
async fn ingest(pool: &PgPool, json: &str) {
    let event = convert_event(json.as_bytes()).expect("convert event");
    let row = event_to_row(event.reborrow()).expect("event row");
    PostgresSink::new(pool.clone())
        .append(&[row])
        .await
        .expect("append");
    project_all(pool).await.expect("project");
}

/// A job in `etl` reading `raw.orders` and writing `marts.daily_orders`,
/// COMPLETE with runId `run-1`.
async fn seeded_store(db: &Db) -> LineageStore {
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z",
            "producer":"test-producer","run":{"runId":"run-1"},
            "job":{"namespace":"etl","name":"build_daily"},
            "inputs":[{"namespace":"raw","name":"orders"}],
            "outputs":[{"namespace":"marts","name":"daily_orders"}]}"#,
    )
    .await;
    LineageStore::new(db.pool.clone())
}

/// A job writing a dataset with a URI-style namespace (`s3://bucket`), seeded via
/// START (carries edges) + COMPLETE (drops them). Exercises C3 (URI nodeId) and
/// C7 (edge union + run state) together.
async fn uri_seeded_store(db: &Db) -> LineageStore {
    ingest(
        &db.pool,
        r#"{"eventType":"START","eventTime":"2023-11-14T22:13:20Z",
            "producer":"p","run":{"runId":"r1","facets":{"nominalTime":{"x":1}}},
            "job":{"namespace":"etl","name":"export"},
            "outputs":[{"namespace":"s3://bucket","name":"warehouse/t1"}]}"#,
    )
    .await;
    // COMPLETE with no datasets — must NOT erase the edge from the START.
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:25Z",
            "producer":"p","run":{"runId":"r1"},
            "job":{"namespace":"etl","name":"export"}}"#,
    )
    .await;
    LineageStore::new(db.pool.clone())
}

/// Two runs writing `warehouse:silver.customers` with column lineage; the newer
/// one maps `id` to `raw:customers.customer_key` (not `.id`), proving the latest
/// facet wins.
async fn column_lineage_seeded_store(db: &Db) -> LineageStore {
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"build_silver"},
            "inputs":[{"namespace":"raw","name":"customers"}],
            "outputs":[{"namespace":"warehouse","name":"silver.customers","facets":{"columnLineage":{"fields":{"id":{"inputFields":[{"namespace":"raw","name":"customers","field":"id","transformations":[{"type":"DIRECT","subtype":"IDENTITY"}]}]}}}}}]}"#,
    )
    .await;
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:25Z","producer":"p",
            "run":{"runId":"r2"},"job":{"namespace":"etl","name":"build_silver"},
            "inputs":[{"namespace":"raw","name":"customers"}],
            "outputs":[{"namespace":"warehouse","name":"silver.customers","facets":{"columnLineage":{"fields":{"id":{"inputFields":[{"namespace":"raw","name":"customers","field":"customer_key","transformations":[{"type":"DIRECT","subtype":"IDENTITY"}]}]},"email_hash":{"inputFields":[{"namespace":"raw","name":"customers","field":"email","transformations":[{"type":"DIRECT","subtype":"TRANSFORMATION"}]}]}}}}}]}"#,
    )
    .await;
    LineageStore::new(db.pool.clone())
}

#[tokio::test]
async fn namespaces_lists_job_and_dataset_namespaces() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let ns = store.namespaces().await.unwrap();
    let names: Vec<&str> = ns.namespaces.iter().map(|n| n.name.as_str()).collect();
    assert!(names.contains(&"etl"), "job namespace present: {names:?}");
    assert!(names.contains(&"raw"), "input ds namespace: {names:?}");
    assert!(names.contains(&"marts"), "output ds namespace: {names:?}");
}

#[tokio::test]
async fn jobs_returns_job_with_inputs_and_outputs() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let jobs = store.jobs(Some("etl"), 100, 0).await.unwrap();
    assert_eq!(jobs.total_count, 1);
    let job = &jobs.jobs[0];
    assert_eq!(job.name, "build_daily");
    assert_eq!(job.inputs.len(), 1);
    assert_eq!(job.inputs[0].namespace, "raw");
    assert_eq!(job.inputs[0].name, "orders");
    assert_eq!(job.outputs[0].name, "daily_orders");
    assert_eq!(job.latest_runs.len(), 1);
    assert_eq!(job.latest_runs[0].id, "run-1");
    assert_eq!(job.latest_runs[0].state, "COMPLETED");
}

#[tokio::test]
async fn datasets_include_job_referenced_tables() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let raw = store.datasets(Some("raw"), 100, 0).await.unwrap();
    assert_eq!(raw.datasets.len(), 1);
    assert_eq!(raw.datasets[0].name, "orders");
    let marts = store.datasets(Some("marts"), 100, 0).await.unwrap();
    assert_eq!(marts.datasets[0].name, "daily_orders");
}

#[tokio::test]
async fn lineage_graph_connects_job_to_its_datasets() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let node = "job:etl:build_daily";
    let graph = store.lineage(node, 2).await.unwrap();
    let ids: Vec<&str> = graph.graph.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&node), "seed job present: {ids:?}");
    assert!(ids.contains(&"dataset:raw:orders"), "input: {ids:?}");
    assert!(
        ids.contains(&"dataset:marts:daily_orders"),
        "output: {ids:?}"
    );

    let job = graph.graph.iter().find(|n| n.id == node).unwrap();
    assert_eq!(job.node_type, "JOB");
    assert_eq!(job.in_edges.len(), 1);
    assert_eq!(job.in_edges[0].origin, "dataset:raw:orders");
    assert_eq!(job.out_edges.len(), 1);
    assert_eq!(job.out_edges[0].destination, "dataset:marts:daily_orders");
}

#[tokio::test]
async fn search_matches_job_and_dataset_names() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let hits = store.search("orders", 100).await.unwrap();
    assert!(hits.total_count >= 2, "got {} hits", hits.total_count);
    assert!(hits.results.iter().all(|r| r.result_type == "DATASET"));
}

#[tokio::test]
async fn empty_db_yields_empty_results() {
    let db = start_postgres().await;
    let store = LineageStore::new(db.pool.clone());
    let ns = store.namespaces().await.unwrap();
    assert!(ns.namespaces.is_empty());
}

#[tokio::test]
async fn missing_job_is_not_found() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let err = store.job("etl", "nope").await.unwrap_err();
    assert!(matches!(err, lineage_service::read::ReadError::NotFound(_)));
}

// --- HTTP-level tests ---------------------------------------------------------

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
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let (status, body) = get(store, "/api/v1/namespaces/etl/jobs?limit=25&offset=0").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("build_daily"), "body: {body}");
    assert!(body.contains("\"totalCount\""), "body: {body}");
}

#[tokio::test]
async fn http_global_jobs_route_resolves() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let (status, body) = get(store, "/api/v1/jobs?limit=25&offset=0").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("build_daily"), "body: {body}");
}

#[tokio::test]
async fn http_job_runs_returns_real_run_state() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
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
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let (status, body) = get(store, "/api/v1/lineage?nodeId=job:etl:build_daily&depth=2").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"outEdges\""), "body: {body}");
    assert!(body.contains("\"inEdges\""), "body: {body}");
    assert!(!body.contains("\"out_edges\""), "snake_case leaked: {body}");
}

#[tokio::test]
async fn http_search_envelope_is_camel_case() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let (status, body) = get(store, "/api/v1/search?q=orders&limit=100").await;
    assert_eq!(status, axum::http::StatusCode::OK);
    assert!(body.contains("\"totalCount\""), "body: {body}");
    assert!(body.contains("\"nodeId\""), "body: {body}");
}

// --- C3: URI-namespace nodeIds round-trip through lineage ---------------------

#[tokio::test]
async fn lineage_resolves_uri_namespace_dataset() {
    let db = start_postgres().await;
    let store = uri_seeded_store(&db).await;
    let node = "dataset:s3://bucket:warehouse/t1";
    let graph = store.lineage(node, 2).await.unwrap();
    let ids: Vec<&str> = graph.graph.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&node), "uri dataset present: {ids:?}");
    assert!(ids.contains(&"job:etl:export"), "connected job: {ids:?}");
    let ds = graph.graph.iter().find(|n| n.id == node).unwrap();
    assert_eq!(ds.node_type, "DATASET");
    let updated = ds.data.get("updatedAt").and_then(|v| v.as_str()).unwrap();
    assert!(!updated.starts_with("1970"), "real timestamp: {updated}");
}

// --- C7.1: edge union (START carries edges, COMPLETE drops them) --------------

#[tokio::test]
async fn complete_without_datasets_does_not_erase_edges() {
    let db = start_postgres().await;
    let store = uri_seeded_store(&db).await;
    let job = store.job("etl", "export").await.unwrap();
    assert_eq!(job.outputs.len(), 1, "START's output survives the COMPLETE");
    assert_eq!(job.outputs[0].namespace, "s3://bucket");
    assert_eq!(job.outputs[0].name, "warehouse/t1");
}

// --- C7.2: real run state from START->COMPLETE --------------------------------

#[tokio::test]
async fn run_state_reflects_terminal_event() {
    let db = start_postgres().await;
    let store = uri_seeded_store(&db).await;
    let runs = store.job_runs("etl", "export").await.unwrap();
    assert_eq!(runs.total_count, 1);
    assert_eq!(runs.runs[0].id, "r1");
    assert_eq!(runs.runs[0].state, "COMPLETED");
    assert_eq!(runs.runs[0].duration_ms, 5_000);
}

// --- C9.1: unknown lineage seed -> 404 ----------------------------------------

#[tokio::test]
async fn lineage_unknown_seed_is_not_found() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let err = store.lineage("dataset:nope:missing", 2).await.unwrap_err();
    assert!(matches!(err, lineage_service::read::ReadError::NotFound(_)));
}

// --- C8: the endpoints marquez-web calls --------------------------------------

#[tokio::test]
async fn http_events_lineage_returns_raw_events() {
    let db = start_postgres().await;
    let store = uri_seeded_store(&db).await;
    let (status, body) = get(store, "/api/v1/events/lineage?limit=10&offset=0").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"events\""), "envelope: {body}");
    assert!(body.contains("\"totalCount\""), "envelope: {body}");
    assert!(
        body.contains("START") || body.contains("COMPLETE"),
        "body: {body}"
    );
}

#[tokio::test]
async fn http_dataset_versions_returns_a_version() {
    let db = start_postgres().await;
    // A schema-bearing event so the dataset has a real version snapshot.
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:00:00Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"marts","name":"daily","facets":{"schema":{"fields":[
                {"name":"id","type":"BIGINT"}]}}}]}"#,
    )
    .await;
    let store = LineageStore::new(db.pool.clone());
    let (status, body) = get(
        store,
        "/api/v1/namespaces/marts/datasets/daily/versions?limit=10&offset=0",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"versions\""), "envelope: {body}");
    assert!(body.contains("\"totalCount\""), "envelope: {body}");
    assert!(body.contains("\"version\""), "version id: {body}");
}

#[tokio::test]
async fn http_dataset_versions_unknown_is_404() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let (status, _body) = get(
        store,
        "/api/v1/namespaces/raw/datasets/nope/versions?limit=10&offset=0",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_run_facets_returns_run_facets() {
    let db = start_postgres().await;
    let store = uri_seeded_store(&db).await;
    let (status, body) = get(store, "/api/v1/jobs/runs/r1/facets").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"runId\""), "envelope: {body}");
    assert!(body.contains("\"facets\""), "envelope: {body}");
    assert!(body.contains("nominalTime"), "facet merged: {body}");
}

#[tokio::test]
async fn http_run_facets_unknown_run_is_404() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let (status, _body) = get(store, "/api/v1/jobs/runs/no-such-run/facets").await;
    assert_eq!(status, axum::http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn http_column_lineage_returns_empty_graph_not_404() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let (status, body) = get(store, "/api/v1/column-lineage?nodeId=dataset:raw:orders").await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("\"graph\""), "envelope: {body}");
}

#[tokio::test]
async fn column_lineage_serves_latest_facet_as_field_graph() {
    let db = start_postgres().await;
    let store = column_lineage_seeded_store(&db).await;
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
}

#[tokio::test]
async fn column_lineage_dataset_field_node_id_filters_to_one_field() {
    let db = start_postgres().await;
    let store = column_lineage_seeded_store(&db).await;
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
    let db = start_postgres().await;
    let store = column_lineage_seeded_store(&db).await;
    let (status, body) = get(
        store,
        "/api/v1/column-lineage?nodeId=dataset:warehouse:silver.customers&depth=20&withDownstream=false",
    )
    .await;
    assert_eq!(status, axum::http::StatusCode::OK, "body: {body}");
    assert!(body.contains("DATASET_FIELD"), "body: {body}");
    assert!(body.contains("\"inEdges\""), "body: {body}");
    assert!(body.contains("\"inputFields\""), "body: {body}");
}

// --- C9.2: search totalCount counts all matches, not the page -----------------

#[tokio::test]
async fn search_total_count_is_full_match_count() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let hits = store.search("orders", 1).await.unwrap();
    assert_eq!(hits.results.len(), 1, "page is truncated to limit");
    assert!(hits.total_count >= 2, "totalCount: {}", hits.total_count);
}

// --- projection determinism: rebuild reproduces identical reads ----------------

#[tokio::test]
async fn rebuild_reproduces_the_same_model() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let before = store.jobs(None, 100, 0).await.unwrap();
    lineage_service::projection::rebuild(&db.pool)
        .await
        .unwrap();
    let after = store.jobs(None, 100, 0).await.unwrap();
    assert_eq!(before.total_count, after.total_count);
    assert_eq!(after.jobs[0].name, "build_daily");
    assert_eq!(after.jobs[0].latest_runs[0].state, "COMPLETED");
}

#[tokio::test]
async fn jobs_and_datasets_carry_current_version_uuid() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let job = store.job("etl", "build_daily").await.unwrap();
    // A real UUID (uuidv7 is 36 chars with hyphens), not an empty placeholder.
    assert_eq!(
        job.current_version.len(),
        36,
        "job currentVersion is a uuid"
    );
    let ds = store.dataset("marts", "daily_orders").await.unwrap();
    assert_eq!(
        ds.current_version.len(),
        36,
        "dataset currentVersion is a uuid"
    );

    // Stable while the shape is unchanged: re-fetching yields the same version.
    let job2 = store.job("etl", "build_daily").await.unwrap();
    assert_eq!(job.current_version, job2.current_version);
}

// --- Phase 1: schema + column-lineage projection tables -----------------------

use sqlx::Row;

#[tokio::test]
async fn schema_facet_populates_dataset_fields() {
    let db = start_postgres().await;
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"warehouse","name":"silver","facets":{"schema":{"fields":[
                {"name":"id","type":"BIGINT"},
                {"name":"email","type":"STRING","description":"addr"}]}}}]}"#,
    )
    .await;
    let rows = sqlx::query(
        "SELECT field, type, description, ordinal FROM dataset_fields \
         WHERE namespace = 'warehouse' AND dataset = 'silver' ORDER BY ordinal",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<String, _>("field"), "id");
    assert_eq!(
        rows[0].get::<Option<String>, _>("type").as_deref(),
        Some("BIGINT")
    );
    assert_eq!(rows[1].get::<String, _>("field"), "email");
    assert_eq!(
        rows[1].get::<Option<String>, _>("description").as_deref(),
        Some("addr")
    );
}

#[tokio::test]
async fn column_lineage_facet_populates_edges() {
    let db = start_postgres().await;
    let store = column_lineage_seeded_store(&db).await;

    // The projected edge table holds the latest mapping (id <- customer_key).
    let rows = sqlx::query(
        "SELECT in_field, out_field FROM column_lineage_edges \
         WHERE out_namespace = 'warehouse' AND out_dataset = 'silver.customers' \
         ORDER BY out_field, in_field",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    let edges: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.get("in_field"), r.get("out_field")))
        .collect();
    // Latest facet: id <- customer_key, email_hash <- email. The older id <- id
    // must have been superseded.
    assert!(edges.contains(&("customer_key".into(), "id".into())));
    assert!(edges.contains(&("email".into(), "email_hash".into())));
    assert!(
        !edges.iter().any(|(i, o)| i == "id" && o == "id"),
        "older id<-id mapping superseded: {edges:?}"
    );

    // The read endpoint reflects the same.
    let graph = store
        .column_lineage("dataset:warehouse:silver.customers")
        .await
        .unwrap()
        .graph;
    let ids: Vec<&str> = graph.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"datasetField:raw:customers:customer_key"));
    assert!(!ids.contains(&"datasetField:raw:customers:id"));
}

// --- Phase 3: parent / nominalTime / documentation / dataSource / lifecycle ---

#[tokio::test]
async fn facet_metadata_populates_run_job_dataset() {
    let db = start_postgres().await;
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z","producer":"p",
            "run":{"runId":"r1","facets":{
                "nominalTime":{"nominalStartTime":"2023-11-14T22:00:00Z","nominalEndTime":"2023-11-14T23:00:00Z"},
                "parent":{"run":{"runId":"parent-run"},"job":{"namespace":"airflow","name":"dag.task"}},
                "errorMessage":{"message":"boom"}}},
            "job":{"namespace":"etl","name":"build","facets":{
                "sourceCodeLocation":{"type":"git","url":"https://git/repo"},
                "jobType":{"processingType":"BATCH","integration":"SPARK","jobType":"QUERY"}}},
            "outputs":[{"namespace":"warehouse","name":"gold","facets":{
                "documentation":{"description":"the gold table"},
                "dataSource":{"name":"warehouse-db","uri":"postgres://h/db"},
                "lifecycleStateChange":{"lifecycleStateChange":"DROP"}}}]}"#,
    )
    .await;
    let store = LineageStore::new(db.pool.clone());

    // Job: location + parent job name.
    let job = store.job("etl", "build").await.unwrap();
    assert_eq!(job.location.as_deref(), Some("https://git/repo"));
    assert_eq!(job.parent_job_name.as_deref(), Some("dag.task"));

    // Run: nominal window surfaced on the latest run.
    let run = &job.latest_runs[0];
    assert!(
        run.nominal_start_time.is_some(),
        "nominal start surfaced: {run:?}"
    );

    // Dataset: description, source_name (from dataSource facet), deleted.
    let ds = store.dataset("warehouse", "gold").await.unwrap();
    assert_eq!(ds.description.as_deref(), Some("the gold table"));
    assert_eq!(ds.source_name, "warehouse-db");
    assert!(ds.deleted, "lifecycleStateChange DROP soft-deleted it");

    // sources catalog row created.
    let src: Option<String> =
        sqlx::query_scalar("SELECT connection_url FROM sources WHERE name = 'warehouse-db'")
            .fetch_optional(&db.pool)
            .await
            .unwrap()
            .flatten();
    assert_eq!(src.as_deref(), Some("postgres://h/db"));
}

#[tokio::test]
async fn facet_metadata_survives_rebuild() {
    let db = start_postgres().await;
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"build","facets":{
                "sourceCodeLocation":{"url":"https://git/repo"}}},
            "outputs":[{"namespace":"w","name":"d","facets":{
                "documentation":{"description":"doc"}}}]}"#,
    )
    .await;
    lineage_service::projection::rebuild(&db.pool)
        .await
        .unwrap();
    let store = LineageStore::new(db.pool.clone());
    assert_eq!(
        store.job("etl", "build").await.unwrap().location.as_deref(),
        Some("https://git/repo")
    );
    assert_eq!(
        store
            .dataset("w", "d")
            .await
            .unwrap()
            .description
            .as_deref(),
        Some("doc")
    );
}

// --- Phase 2: dataset versioning ---------------------------------------------

#[tokio::test]
async fn schema_evolution_produces_multiple_versions() {
    let db = start_postgres().await;
    // v1: id only.
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:00:00Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"w","name":"d","facets":{"schema":{"fields":[
                {"name":"id","type":"BIGINT"}]}}}]}"#,
    )
    .await;
    // v2: id + email (schema changed).
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T23:00:00Z","producer":"p",
            "run":{"runId":"r2"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"w","name":"d","facets":{"schema":{"fields":[
                {"name":"id","type":"BIGINT"},{"name":"email","type":"STRING"}]}}}]}"#,
    )
    .await;
    // A third event re-emitting v2's schema must NOT add a third version.
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T23:30:00Z","producer":"p",
            "run":{"runId":"r3"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"w","name":"d","facets":{"schema":{"fields":[
                {"name":"id","type":"BIGINT"},{"name":"email","type":"STRING"}]}}}]}"#,
    )
    .await;

    let store = LineageStore::new(db.pool.clone());
    let versions = store.dataset_versions("w", "d", 100, 0).await.unwrap();
    assert_eq!(
        versions.total_count, 2,
        "two distinct schemas -> two versions"
    );
    // Newest first: v2 (id+email) before v1 (id).
    assert_eq!(versions.versions[0].fields.len(), 2);
    assert_eq!(versions.versions[1].fields.len(), 1);
    // Distinct version ids.
    assert_ne!(versions.versions[0].version, versions.versions[1].version);
}

#[tokio::test]
async fn dataset_versions_unknown_dataset_is_not_found() {
    let db = start_postgres().await;
    let store = seeded_store(&db).await;
    let err = store
        .dataset_versions("w", "nope", 100, 0)
        .await
        .unwrap_err();
    assert!(matches!(err, lineage_service::read::ReadError::NotFound(_)));
}

#[tokio::test]
async fn dataset_versions_survive_rebuild() {
    let db = start_postgres().await;
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:00:00Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"w","name":"d","facets":{"schema":{"fields":[
                {"name":"id","type":"BIGINT"}]}}}]}"#,
    )
    .await;
    let store = LineageStore::new(db.pool.clone());
    let before = store.dataset_versions("w", "d", 100, 0).await.unwrap();
    lineage_service::projection::rebuild(&db.pool)
        .await
        .unwrap();
    let after = store.dataset_versions("w", "d", 100, 0).await.unwrap();
    assert_eq!(before.total_count, after.total_count);
    assert_eq!(before.versions[0].version, after.versions[0].version);
}

#[tokio::test]
async fn column_edges_survive_rebuild_with_latest_wins() {
    let db = start_postgres().await;
    let _ = column_lineage_seeded_store(&db).await;
    lineage_service::projection::rebuild(&db.pool)
        .await
        .unwrap();
    let rows = sqlx::query(
        "SELECT in_field, out_field FROM column_lineage_edges \
         WHERE out_field = 'id'",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    // After a full replay, still exactly the latest mapping for `id`.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("in_field"), "customer_key");
}

// --- Phase 4: tags + PII propagation + stats --------------------------------

#[tokio::test]
async fn tags_catalog_and_dataset_assignment() {
    let db = start_postgres().await;
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:00:00Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"w","name":"gold","facets":{
                "tags":{"tags":[{"key":"certified"}]}}}]}"#,
    )
    .await;
    let store = LineageStore::new(db.pool.clone());

    let tags = store.tags().await.unwrap();
    assert!(tags.tags.iter().any(|t| t.name == "certified"));

    let ds = store.dataset("w", "gold").await.unwrap();
    assert!(ds.tags.contains(&"certified".to_string()));
}

/// A scanner discovers PII in `raw.users.email` (a synthetic DatasetEvent
/// carrying a field-level tag), and a downstream job copies that column into
/// `gold.email_hash`. Propagation must report the downstream field as reached.
#[tokio::test]
async fn pii_propagates_downstream_through_column_lineage() {
    let db = start_postgres().await;
    // 1. The lineage: raw.users.email -> gold.contact (column lineage).
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:00:00Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"build_gold"},
            "inputs":[{"namespace":"raw","name":"users","facets":{"schema":{"fields":[
                {"name":"email","type":"STRING"}]}}}],
            "outputs":[{"namespace":"w","name":"gold","facets":{
                "schema":{"fields":[{"name":"contact","type":"STRING"}]},
                "columnLineage":{"fields":{"contact":{"inputFields":[
                    {"namespace":"raw","name":"users","field":"email"}]}}}}}]}"#,
    )
    .await;
    // 2. The discovered fact: raw.users.email is PII (synthetic dataset event
    //    with a field-level tag in its schema facet).
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:05:00Z","producer":"pii-scanner",
            "dataset":{"namespace":"raw","name":"users","facets":{"schema":{"fields":[
                {"name":"email","type":"STRING","tags":[{"key":"pii"}]}]}}}}"#,
    )
    .await;

    let store = LineageStore::new(db.pool.clone());
    let prop = store.tag_downstream("pii").await.unwrap();
    let reached: Vec<String> = prop.fields.iter().map(|f| f.node_id.clone()).collect();
    // The tagged seed and the downstream field it flows into.
    assert!(
        reached.contains(&"datasetField:raw:users:email".to_string()),
        "seed present: {reached:?}"
    );
    assert!(
        reached.contains(&"datasetField:w:gold:contact".to_string()),
        "downstream field reached: {reached:?}"
    );
}

#[tokio::test]
async fn stats_lineage_events_buckets_by_day() {
    let db = start_postgres().await;
    for day in ["2023-11-14", "2023-11-14", "2023-11-15"] {
        ingest(
            &db.pool,
            &format!(
                r#"{{"eventType":"COMPLETE","eventTime":"{day}T10:00:00Z","producer":"p",
                    "run":{{"runId":"{day}"}},"job":{{"namespace":"etl","name":"j"}}}}"#
            ),
        )
        .await;
    }
    let store = LineageStore::new(db.pool.clone());
    let buckets = store.stats_lineage_events("day", 30).await.unwrap();
    // Two day-buckets; the 14th has 2 events, the 15th has 1.
    let total: i64 = buckets.iter().map(|b| b.count).sum();
    assert_eq!(total, 3);
    assert_eq!(buckets.len(), 2);
}

#[tokio::test]
async fn tag_assignments_survive_rebuild() {
    let db = start_postgres().await;
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:00:00Z","producer":"p",
            "dataset":{"namespace":"raw","name":"users","facets":{"schema":{"fields":[
                {"name":"email","type":"STRING","tags":[{"key":"pii"}]}]}}}}"#,
    )
    .await;
    lineage_service::projection::rebuild(&db.pool)
        .await
        .unwrap();
    let store = LineageStore::new(db.pool.clone());
    let prop = store.tag_downstream("pii").await.unwrap();
    assert!(
        prop.fields.iter().any(|f| f.field == "email"),
        "tag survives rebuild: {:?}",
        prop.fields
    );
}
