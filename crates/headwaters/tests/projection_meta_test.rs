//! Postgres-backed tests for the dataset meta-upsert fix: a dataset facet (e.g.
//! a `lifecycleStateChange: DROP`) must survive even when its event is the
//! dataset's only appearance — i.e. folded before any schema/edge event creates
//! the `datasets` row — and re-folding (replay/rebuild) must be idempotent.
//!
//! Before the fix, `set_dataset_meta` was a bare `UPDATE … WHERE` that touched 0
//! rows when the row didn't exist yet, silently dropping the metadata. It is now
//! `INSERT … ON CONFLICT DO UPDATE` (stub-inserting the row) guarded latest-wins
//! by `meta_at`. (`set_run_meta` keeps its bare UPDATE: the runs row is always
//! created by the co-emitted `UpsertRunState` in the same event — see the
//! comment there.)
//!
//! Needs Docker — gated behind the `postgres-it` feature:
//!   cargo test -p headwaters --features postgres-it --test projection_meta_test
#![cfg(feature = "postgres-it")]

use headwaters::ingest::convert_event;
use headwaters::projection::{project_all, rebuild};
use headwaters::writer::postgres::PostgresSink;
use headwaters::writer::row::event_to_row;
use headwaters::writer::sink::EventSink;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

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

/// Append one OpenLineage JSON document to the `events` log (no projection yet),
/// so tests control fold ordering explicitly via `project_all`.
async fn append(pool: &PgPool, json: &str) {
    let event = convert_event(json.as_bytes()).expect("convert event");
    let row = event_to_row(event.reborrow()).expect("event row");
    PostgresSink::new(pool.clone())
        .append(&[row])
        .await
        .expect("append");
}

/// A standalone DatasetEvent that soft-deletes `warehouse.silver` via a
/// `lifecycleStateChange: DROP` facet — and carries no schema/edge, so nothing
/// else creates the datasets row.
const DROP_EVENT: &str = r#"{"eventType":"DROP","eventTime":"2023-11-14T22:13:20Z",
    "producer":"p",
    "dataset":{"namespace":"warehouse","name":"silver",
        "facets":{"lifecycleStateChange":{"lifecycleStateChange":"DROP"}}}}"#;

async fn dataset_deleted(pool: &PgPool) -> Option<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT deleted FROM datasets WHERE namespace = 'warehouse' AND name = 'silver'",
    )
    .fetch_optional(pool)
    .await
    .expect("query datasets")
}

#[tokio::test]
async fn dataset_drop_before_any_other_event_is_recorded() {
    let db = start_postgres().await;
    // The DROP is the very first thing this dataset ever sees: no schema, no
    // edge, no prior datasets row. Before the fix this UPDATE hit 0 rows.
    append(&db.pool, DROP_EVENT).await;
    project_all(&db.pool).await.expect("project");

    assert_eq!(
        dataset_deleted(&db.pool).await,
        Some(true),
        "DROP arriving before any row-creating event must still soft-delete"
    );
}

#[tokio::test]
async fn meta_upserts_are_idempotent_across_rebuild() {
    let db = start_postgres().await;
    append(&db.pool, DROP_EVENT).await;
    // A later schema event for the same dataset — the row now also exists via
    // note_dataset; the earlier DROP must remain applied.
    append(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:25Z","producer":"p",
            "run":{"runId":"r1"},"job":{"namespace":"etl","name":"j"},
            "outputs":[{"namespace":"warehouse","name":"silver",
                "facets":{"schema":{"fields":[{"name":"id","type":"BIGINT"}]}}}]}"#,
    )
    .await;
    project_all(&db.pool).await.expect("project");
    assert_eq!(
        dataset_deleted(&db.pool).await,
        Some(true),
        "deleted before rebuild"
    );

    // Re-folding the whole log from scratch reproduces the same result.
    rebuild(&db.pool).await.expect("rebuild");
    assert_eq!(
        dataset_deleted(&db.pool).await,
        Some(true),
        "deleted survives a full rebuild (idempotent meta fold)"
    );
}
