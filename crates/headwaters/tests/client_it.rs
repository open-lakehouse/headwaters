//! End-to-end test of the `headwaters-client` Rust client against a live server.
//!
//! Boots a throwaway Postgres (testcontainers), ingests + projects a small
//! lineage graph, serves the real `headwaters` router (REST + ConnectRPC) on an
//! ephemeral port, then drives [`HeadwatersClient`] over ConnectRPC and asserts
//! the responses round-trip. This is the client's contract test: it exercises a
//! zero-arg RPC, a path-param RPC, and the opaque-`Struct` graph payload through
//! the generated transport.
//!
//! Needs Docker — gated behind `postgres-it` like the other integration tests.
//!   cargo test -p headwaters --features postgres-it --test client_it
#![cfg(feature = "postgres-it")]

use std::net::SocketAddr;

use headwaters::http::{self, AppState};
use headwaters::ingest::convert_event;
use headwaters::projection::project_all;
use headwaters::read::LineageStore;
use headwaters::writer::buffered::{BufferedWriter, BufferedWriterConfig};
use headwaters::writer::postgres::PostgresSink;
use headwaters::writer::row::event_to_row;
use headwaters::writer::sink::EventSink;
use headwaters_client::{EntityKind, HeadwatersClient, dataset_node_id};
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

async fn ingest(pool: &PgPool, json: &str) {
    let event = convert_event(json.as_bytes()).expect("convert event");
    let row = event_to_row(event.reborrow()).expect("event row");
    PostgresSink::new(pool.clone())
        .append(&[row])
        .await
        .expect("append");
    project_all(pool).await.expect("project");
}

/// Serve the real router on an ephemeral loopback port; return its base URL.
/// The server task is detached and dies with the test process.
async fn serve(pool: PgPool) -> String {
    let writer = BufferedWriter::spawn(Vec::new(), BufferedWriterConfig::default());
    let app = http::router(
        AppState {
            writer: writer.handle(),
            store: LineageStore::new(pool),
        },
        "",
        true,
    );
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn client_round_trips_read_api() {
    let db = start_postgres().await;
    // A job in `etl` reading `raw.orders`, writing `marts.daily_orders`.
    ingest(
        &db.pool,
        r#"{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z",
            "producer":"test","run":{"runId":"run-1"},
            "job":{"namespace":"etl","name":"build_daily"},
            "inputs":[{"namespace":"raw","name":"orders"}],
            "outputs":[{"namespace":"marts","name":"daily_orders"}]}"#,
    )
    .await;

    let base = serve(db.pool.clone()).await;
    let client = HeadwatersClient::connect(&base).expect("client");

    // Zero-arg RPC.
    let namespaces = client.list_namespaces().await.expect("list_namespaces");
    let names: Vec<&str> = namespaces
        .namespaces
        .iter()
        .map(|n| n.name.as_str())
        .collect();
    assert!(names.contains(&"etl"), "namespaces: {names:?}");
    assert!(names.contains(&"raw"), "namespaces: {names:?}");

    // Path-param RPC returning a typed enum field.
    let dataset = client
        .get_dataset("marts", "daily_orders")
        .await
        .expect("get_dataset");
    assert_eq!(dataset.name, "daily_orders");
    assert_eq!(dataset.r#type, headwaters_client::DatasetType::DB_TABLE);

    // Graph RPC: the opaque `data` Struct round-trips, and the seed node is present.
    let node = dataset_node_id("marts", "daily_orders");
    let graph = client.get_lineage(&node, 2).await.expect("get_lineage");
    let ids: Vec<&str> = graph.graph.iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&node.as_str()), "seed node present: {ids:?}");
    assert!(
        ids.contains(&"job:etl:build_daily"),
        "connected job present: {ids:?}"
    );

    // Phase-1 Search filter reaches the server: jobs-only excludes the dataset.
    let jobs = client
        .search("daily", 50, EntityKind::JOB, "")
        .await
        .expect("search");
    assert!(
        jobs.results.iter().all(|r| r.r#type == EntityKind::JOB),
        "kind filter honored: {jobs:?}"
    );

    // Not-found maps to a typed error.
    let err = client.get_dataset("marts", "nope").await.unwrap_err();
    assert!(err.is_not_found(), "expected not_found, got {err:?}");
}
