//! Shared test scaffolding for the Postgres-backed read/projection tests.
//!
//! Compiled only under `cfg(test)`, so it is invisible to the shipped binary and
//! can lean on the `testcontainers` dev-dependency. It lives in `src/` (not
//! `tests/common/`) because the ConnectRPC handler tests in [`crate::read::connect`]
//! exercise crate-private types (`crate::proto`, `crate::connect_gen`) and so
//! must run *inside* the crate — an external `tests/` integration crate cannot
//! reach them. Inline `src` tests cannot import a `tests/common/` module, hence
//! the shared bootstrap lives here as `pub(crate)`.
//!
//! These helpers need Docker (a Postgres container). Callers gate themselves
//! behind the `postgres-it` feature; this module is only referenced from those
//! gated test modules, so it never compiles into a default test run.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use crate::ingest::convert_event;
use crate::projection::project_all;
use crate::read::LineageStore;
use crate::writer::postgres::PostgresSink;
use crate::writer::row::event_to_row;
use crate::writer::sink::EventSink;

/// Postgres image tag used across the integration tests. One constant so a bump
/// touches a single line.
pub(crate) const POSTGRES_TAG: &str = "16-alpine";

/// A running Postgres + a connected, migrated pool. Holds the container so it
/// outlives the test.
pub(crate) struct Db {
    _container: ContainerAsync<GenericImage>,
    pub(crate) pool: PgPool,
}

/// Boot a throwaway Postgres (testcontainers), connect, and run the migrations.
pub(crate) async fn start_postgres() -> Db {
    let container = GenericImage::new("postgres", POSTGRES_TAG)
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
    // Reuse the one embedded migrator the server uses, so tests exercise exactly
    // the migration set `migrate`/`serve` see.
    crate::run::MIGRATOR
        .run(&pool)
        .await
        .expect("run migrations");
    Db {
        _container: container,
        pool,
    }
}

/// Ingest one OpenLineage JSON document the way the HTTP handler does, then fold
/// the projection so the read tables reflect it.
pub(crate) async fn ingest(pool: &PgPool, json: &str) {
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
pub(crate) async fn seeded_store(db: &Db) -> LineageStore {
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
pub(crate) async fn uri_seeded_store(db: &Db) -> LineageStore {
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
pub(crate) async fn column_lineage_seeded_store(db: &Db) -> LineageStore {
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
