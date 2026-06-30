//! Server lifecycle entry point.
//!
//! [`run`] is the whole standalone-server body — connect Postgres, verify the
//! schema is current, spawn the buffered writer and projector, serve HTTP with
//! graceful shutdown, then drain. It lives in the library (not `main.rs`) so the
//! binary, an embedder, or the CLI can all share one code path. Tracing
//! initialization stays in the *binary* so this never fights a host that already
//! installed a subscriber.
//!
//! `run` deliberately does *not* apply migrations — it fails fast if the schema
//! is behind. Applying migrations is a separate, explicit step: [`migrate`]
//! (the `migrate` CLI subcommand). This keeps schema mutation out of the startup
//! hot path so that multiple instances booting at once don't all race to migrate.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::migrate::Migrate;
use sqlx::postgres::PgPoolOptions;

use crate::config::{Config, WriterConfig};
use crate::http::{self, AppState};
use crate::projection::Projector;
use crate::read::LineageStore;
use crate::writer::buffered::{BufferedWriter, BufferedWriterConfig};
use crate::writer::postgres::PostgresSink;
use crate::writer::sink::EventSink;

/// Upper bound on the graceful-shutdown drain of the buffered writer. The drain
/// retries a failing sink, so without a cap a dead Postgres would hang process
/// exit; this keeps termination within a typical orchestrator grace period.
const WRITER_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The embedded migration set, scanned from `migrations/` at compile time.
///
/// Shared by [`migrate`] (which applies pending migrations), the [`run`]
/// startup check (which only verifies the schema is current), and the test
/// harness — so all three agree on exactly which migrations exist.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

/// Connect a Postgres pool sized per [`Config`]. Shared by [`run`] and [`migrate`].
async fn connect_pool(cfg: &Config) -> anyhow::Result<sqlx::PgPool> {
    let url = cfg
        .postgres
        .resolve_url()
        .context("invalid configuration")?;
    PgPoolOptions::new()
        .max_connections(cfg.postgres.pool_size)
        .connect(url)
        .await
        .context("failed to connect to Postgres")
}

/// Connect to Postgres and apply any pending migrations, then return.
///
/// This is the body of the `migrate` CLI subcommand. It is the *only* path that
/// mutates the schema; [`run`] just checks it. Applying an already-current schema
/// is a no-op, so running this repeatedly (e.g. once per deploy) is safe.
pub async fn migrate(cfg: Config) -> anyhow::Result<()> {
    let pool = connect_pool(&cfg).await?;
    tracing::info!("applying database migrations");
    MIGRATOR
        .run(&pool)
        .await
        .context("failed to run database migrations")?;
    tracing::info!("database migrations up to date");
    pool.close().await;
    Ok(())
}

/// Verify the database schema is current, returning an error if it is not.
///
/// Unlike [`migrate`], this never mutates the schema: it lists applied
/// migrations and fails fast if any embedded migration has not been applied,
/// pointing the operator at `headwaters migrate`. This keeps schema changes an
/// explicit, deliberate step rather than a side effect of every server boot.
async fn ensure_schema_current(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    // Creating the bookkeeping table if absent is not a schema migration; it lets
    // `list_applied_migrations` succeed against a brand-new database (where it
    // then reports zero applied, i.e. every migration pending).
    conn.ensure_migrations_table().await?;
    let applied: std::collections::HashSet<i64> = conn
        .list_applied_migrations()
        .await?
        .into_iter()
        .map(|m| m.version)
        .collect();
    let pending: Vec<i64> = MIGRATOR
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| m.version)
        .filter(|v| !applied.contains(v))
        .collect();
    if !pending.is_empty() {
        anyhow::bail!(
            "database schema is behind: {} migration(s) pending ({pending:?}). \
             Run `headwaters migrate` before starting the server.",
            pending.len()
        );
    }
    Ok(())
}

/// Run the server to completion against a fully-resolved [`Config`].
///
/// Connects a Postgres pool (shared by the sink, projector, and read store),
/// verifies the schema is current (erroring if any migration is pending —
/// see [`migrate`]), spawns the buffered writer and async projector, serves the
/// HTTP + ConnectRPC surface on [`Config::bind_addr`] with graceful shutdown on
/// SIGTERM/Ctrl+C, then drains the writer and stops the projector.
///
/// The caller is responsible for initializing tracing before calling this.
pub async fn run(cfg: Config) -> anyhow::Result<()> {
    // One pool shared by the sink, the projector, and the read store.
    let pool = connect_pool(&cfg).await?;

    // Verify (don't apply) the schema: migrations are an explicit `migrate` step.
    ensure_schema_current(&pool)
        .await
        .context("database schema check failed")?;

    // Write path: buffered ingest -> Postgres `events` (append-only).
    let sinks: Vec<Arc<dyn EventSink>> = vec![Arc::new(PostgresSink::new(pool.clone()))];
    let writer = BufferedWriter::spawn(sinks, writer_config(&cfg.writer));

    // Async projection: fold `events` into the read tables.
    let projector = Projector::spawn(
        pool.clone(),
        Duration::from_millis(cfg.postgres.projection_interval_ms),
    );

    let store = LineageStore::new(pool.clone());
    let app = http::router(
        AppState {
            writer: writer.handle(),
            store,
        },
        &cfg.ui.base_path,
        cfg.ui.serve,
    );

    let addr = cfg.bind_addr();
    tracing::info!("headwaters listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    // The server has stopped accepting requests and dropped its handler state
    // (and the writer handle inside it), so the channel can now close. Drain
    // buffered events, then stop the projector after a final fold. The drain
    // retries a failing sink, so bound it: a dead Postgres must not wedge exit
    // past the orchestrator's termination grace period.
    tracing::info!("draining buffered writer");
    writer.shutdown(WRITER_DRAIN_TIMEOUT).await;
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
        // Flush retry/backoff use the built-in defaults; not yet exposed as
        // config knobs.
        ..BufferedWriterConfig::default()
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
        _ = ctrl_c => tracing::info!("received Ctrl+C, shutting down gracefully"),
        _ = terminate => tracing::info!("received SIGTERM, shutting down gracefully"),
    }
}
