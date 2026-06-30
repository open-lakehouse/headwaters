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
/// Unlike [`migrate`], this never *applies* migrations — but it runs the same
/// pre-flight validations [`sqlx::migrate::Migrator::run`] does, so a server that
/// starts is guaranteed to be on exactly the schema its binary expects:
///
/// - a **dirty** (failed/partially-applied) migration → refuse to start;
/// - an applied migration whose **checksum** no longer matches the embedded SQL
///   (the file was edited after deploy) → refuse to start;
/// - an applied migration **missing** from this binary (the DB is ahead, e.g. a
///   downgrade) → refuse to start;
/// - any embedded migration **not yet applied** → refuse to start, pointing the
///   operator at `headwaters migrate`.
///
/// It takes the migrator's advisory lock for the read so it can't observe a
/// half-applied state mid-`migrate`, and releases it before returning. It does
/// not create the `_sqlx_migrations` table: an absent table simply means every
/// migration is pending, so the check works against a read-only connection too.
async fn ensure_schema_current(pool: &sqlx::PgPool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;

    // No `_sqlx_migrations` table means a brand-new database: nothing applied, so
    // every migration is pending. Probe with a read-only `to_regclass` rather than
    // creating the table (`ensure_migrations_table` would `CREATE TABLE`, which
    // fails on a read-only replica) — and skip the advisory lock, since there is
    // nothing yet to race a concurrent `migrate` over.
    let table_exists: Option<String> =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations')::text")
            .fetch_one(&mut *conn)
            .await?;
    if table_exists.is_none() {
        let pending = MIGRATOR
            .iter()
            .filter(|m| !m.migration_type.is_down_migration())
            .count();
        anyhow::bail!(
            "database schema is behind: {pending} migration(s) pending. \
             Run `headwaters migrate` before starting the server."
        );
    }

    // Serialize against a concurrent `migrate` (which holds this same lock while
    // applying), so we read a consistent applied-set rather than a partial one.
    conn.lock().await?;
    let result = check_applied(&mut *conn).await;
    // Always unlock, even on error, so a failed check never strands the lock.
    let unlock = conn.unlock().await;
    result.and(unlock.map_err(Into::into))
}

/// The body of [`ensure_schema_current`] once the `_sqlx_migrations` table is
/// known to exist, factored out so the caller can release the advisory lock
/// regardless of outcome.
async fn check_applied(conn: &mut impl Migrate) -> anyhow::Result<()> {
    use std::collections::HashMap;

    // A dirty version means a prior `migrate` failed partway; the schema is in an
    // unknown state and must be repaired before serving.
    if let Some(version) = conn.dirty_version().await? {
        anyhow::bail!(
            "database has a dirty (partially-applied) migration at version {version}. \
             Resolve it before starting the server."
        );
    }

    let applied: HashMap<i64, _> = conn
        .list_applied_migrations()
        .await?
        .into_iter()
        .map(|m| (m.version, m))
        .collect();

    // The DB must not be ahead of this binary: an applied migration we don't know
    // about means a newer binary migrated it (e.g. we are a downgrade).
    for &version in applied.keys() {
        if !MIGRATOR.version_exists(version) {
            anyhow::bail!(
                "database has migration version {version} applied that this binary does \
                 not know about — the database is ahead of this build (a downgrade?)."
            );
        }
    }

    let mut pending: Vec<i64> = Vec::new();
    for migration in MIGRATOR
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
    {
        match applied.get(&migration.version) {
            // Applied, but the embedded SQL was edited after the fact → drift.
            Some(applied) if applied.checksum != migration.checksum => anyhow::bail!(
                "database migration version {} does not match the embedded migration \
                 (checksum mismatch) — the migration file was changed after it was applied.",
                migration.version
            ),
            Some(_) => {}
            None => pending.push(migration.version),
        }
    }

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

#[cfg(all(test, feature = "postgres-it"))]
mod schema_check_tests {
    //! Postgres-backed tests for the `serve`-time schema check. Needs Docker;
    //! gated behind `postgres-it`. Run with:
    //!   cargo test -p headwaters --features postgres-it schema_check
    use super::*;
    use crate::test_support::{start_postgres, start_postgres_unmigrated};

    #[tokio::test]
    async fn passes_against_a_migrated_database() {
        let db = start_postgres().await;
        ensure_schema_current(&db.pool)
            .await
            .expect("a fully-migrated schema must pass the check");
    }

    #[tokio::test]
    async fn errors_when_migrations_are_pending() {
        // A fresh database has no migrations applied, so every embedded migration
        // is pending and the check must refuse to start.
        let db = start_postgres_unmigrated().await;
        let err = ensure_schema_current(&db.pool)
            .await
            .expect_err("an un-migrated schema must fail the check");
        assert!(
            err.to_string().contains("pending"),
            "error should name pending migrations, got: {err}"
        );
    }

    #[tokio::test]
    async fn errors_on_checksum_drift() {
        // Apply migrations, then tamper with a recorded checksum to simulate an
        // embedded migration file being edited after it was applied.
        let db = start_postgres().await;
        let version = MIGRATOR
            .iter()
            .next()
            .expect("at least one migration")
            .version;
        sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = $2")
            .bind(vec![0u8; 4])
            .bind(version)
            .execute(&db.pool)
            .await
            .expect("tamper checksum");
        let err = ensure_schema_current(&db.pool)
            .await
            .expect_err("a checksum mismatch must fail the check");
        assert!(
            err.to_string().contains("checksum mismatch"),
            "error should name the checksum mismatch, got: {err}"
        );
    }

    #[tokio::test]
    async fn errors_when_database_is_ahead() {
        // Record a migration version this binary does not know about, simulating a
        // database migrated by a newer build (a downgrade).
        let db = start_postgres().await;
        let unknown = MIGRATOR.iter().map(|m| m.version).max().unwrap_or(0) + 1;
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
             (version, description, installed_on, success, checksum, execution_time) \
             VALUES ($1, 'future', now(), true, $2, 0)",
        )
        .bind(unknown)
        .bind(vec![0u8; 4])
        .execute(&db.pool)
        .await
        .expect("insert future migration");
        let err = ensure_schema_current(&db.pool)
            .await
            .expect_err("a database ahead of the binary must fail the check");
        assert!(
            err.to_string().contains("ahead of this build"),
            "error should say the database is ahead, got: {err}"
        );
    }
}
