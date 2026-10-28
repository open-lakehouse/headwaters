//! Embedding surface: the running service without the socket.
//!
//! [`Service::start`] does everything [`run`](crate::run) does before binding a
//! listener — connect the pool, verify the schema is current, spawn the
//! buffered writer and the projector — and hands back the [`Router`]. The
//! caller decides how requests reach it: [`run`](crate::run) serves it over
//! TCP, an in-process host (e.g. a Node-API addon) drives it with
//! `tower::ServiceExt::oneshot`. Either way, call [`Service::shutdown`] when
//! done so buffered events are flushed.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use sqlx::PgPool;

use crate::config::Config;
use crate::http::{self, AppState};
use crate::projection::Projector;
use crate::read::LineageStore;
use crate::run::{connect_pool, connect_url, ensure_schema_current, writer_config};
use crate::writer::buffered::BufferedWriter;
use crate::writer::postgres::PostgresSink;
use crate::writer::sink::EventSink;

/// A started headwaters service: background workers running, router ready.
pub struct Service {
    router: Router,
    writer: BufferedWriter,
    projector: Projector,
    pool: PgPool,
}

impl Service {
    /// Connect, verify the schema, spawn the workers, and build the router.
    ///
    /// Must be called within a tokio runtime; the writer and projector are
    /// spawned onto it. Fails if any migration is pending — migrations are an
    /// explicit [`migrate`](crate::migrate) step, never applied here.
    pub async fn start(cfg: &Config) -> anyhow::Result<Self> {
        // One pool shared by the sink, the projector, and the read store.
        let pool = connect_pool(cfg).await?;

        // Verify (don't apply) the schema. A separate schema-check DSN gets its
        // own short-lived connection (see `PostgresConfig::schema_check_url`).
        match cfg.postgres.schema_check_url.as_deref() {
            Some(url) if !url.is_empty() => {
                let check = connect_url(url, 1).await?;
                let result = ensure_schema_current(&check).await;
                check.close().await;
                result
            }
            _ => ensure_schema_current(&pool).await,
        }
        .context("database schema check failed")?;

        // Write path: buffered ingest -> Postgres `events` (append-only).
        let sinks: Vec<Arc<dyn EventSink>> = vec![Arc::new(PostgresSink::new(pool.clone()))];
        let writer = BufferedWriter::spawn(sinks, writer_config(&cfg.writer));

        // Async projection: fold `events` into the read tables.
        let projector = Projector::spawn(
            pool.clone(),
            Duration::from_millis(cfg.postgres.projection_interval_ms),
        );

        let router = http::router(
            AppState {
                writer: writer.handle(),
                store: LineageStore::new(pool.clone()),
            },
            &cfg.ui.base_path,
            cfg.ui.serve,
        );

        Ok(Self {
            router,
            writer,
            projector,
            pool,
        })
    }

    /// The HTTP + ConnectRPC router. Cheap to clone; each clone shares the
    /// same writer and read store.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Drain buffered events (bounded by `drain_timeout`), stop the projector
    /// after a final fold, and close the pool.
    ///
    /// The writer only drains once every handle is gone, so drop every
    /// [`router`](Self::router) clone (e.g. stop the server) before calling.
    pub async fn shutdown(self, drain_timeout: Duration) {
        // Our own router clone holds a writer handle too; drop it first.
        drop(self.router);
        tracing::info!("draining buffered writer");
        self.writer.shutdown(drain_timeout).await;
        tracing::info!("stopping projection worker");
        self.projector.shutdown().await;
        self.pool.close().await;
    }
}
