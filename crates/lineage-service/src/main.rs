use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use lineage_service::config::{Config, WriterConfig};
use lineage_service::http::{self, AppState};
use lineage_service::projection::Projector;
use lineage_service::read::LineageStore;
use lineage_service::writer::buffered::{BufferedWriter, BufferedWriterConfig};
use lineage_service::writer::postgres::PostgresSink;
use lineage_service::writer::sink::EventSink;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Config file path: first positional arg, else the LINEAGE_CONFIG env var
    // (handled inside Config::load). With neither, run on defaults + LINEAGE__*
    // env overrides (DATABASE_URL still supplies the DSN).
    let config_path = std::env::args().nth(1);
    let cfg = Config::load(config_path.as_ref()).context("invalid configuration")?;

    // One pool shared by the sink, the projector, and the read store.
    let url = cfg
        .postgres
        .resolve_url()
        .context("invalid configuration")?;
    let pool = PgPoolOptions::new()
        .max_connections(cfg.postgres.pool_size)
        .connect(url)
        .await
        .context("failed to connect to Postgres")?;

    sqlx::migrate!()
        .run(&pool)
        .await
        .context("failed to run database migrations")?;

    // Write path: buffered ingest -> Postgres `events` (append-only).
    let sinks: Vec<Arc<dyn EventSink>> = vec![Arc::new(PostgresSink::new(pool.clone()))];
    let writer = BufferedWriter::spawn(sinks, writer_config(&cfg.writer));

    // Async projection: fold `events` into the read tables.
    let projector = Projector::spawn(
        pool.clone(),
        Duration::from_millis(cfg.postgres.projection_interval_ms),
    );

    let store = LineageStore::new(pool.clone());
    let app = http::router(AppState {
        writer: writer.handle(),
        store,
    });

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("lineage-service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    // The server has stopped accepting requests and dropped its handler state
    // (and the writer handle inside it), so the channel can now close. Drain
    // buffered events, then stop the projector after a final fold.
    tracing::info!("draining buffered writer");
    writer.shutdown().await;
    tracing::info!("stopping projection worker");
    projector.shutdown().await;
    pool.close().await;
    Ok(())
}

fn writer_config(cfg: &WriterConfig) -> BufferedWriterConfig {
    BufferedWriterConfig {
        buffer_size: cfg.buffer_size,
        flush_interval: Duration::from_millis(cfg.flush_interval_ms),
        channel_capacity: cfg.channel_capacity,
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
