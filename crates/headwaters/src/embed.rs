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

use crate::config::{Config, ProjectionMode};
use crate::http::{self, AppState, ProjectionTrigger};
use crate::projection::{ProjectionHandle, ProjectionRun, Projector};
use crate::read::LineageStore;
use crate::run::{connect_pool, connect_url, ensure_schema_current, writer_config};
use crate::writer::buffered::{BufferedWriter, BufferedWriterHandle};
use crate::writer::postgres::PostgresSink;
use crate::writer::sink::EventSink;

/// A started headwaters service: background workers running, router ready.
pub struct Service {
    router: Router,
    writer: BufferedWriter,
    projector: Projector,
    settler: Settler,
    pool: PgPool,
}

/// Flushes and folds on demand: see [`Settler::settle`]. Cheap to clone. Holds
/// a writer handle, so like [`Service::router`] clones, drop every one before
/// [`Service::shutdown`].
#[derive(Clone)]
pub struct Settler {
    writer: BufferedWriterHandle,
    projection: ProjectionHandle,
}

impl Settler {
    /// Wait for buffered events to be written, then fold the log for up to
    /// `budget` (see [`ProjectionHandle::settle`]). A host that runs
    /// projection on demand calls this after ingest and from its scheduler.
    pub async fn settle(&self, budget: Duration) -> Result<ProjectionRun, sqlx::Error> {
        self.projection.settle(&self.writer, budget).await
    }
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

        // Async projection: fold `events` into the read tables, from a polling
        // task or only when asked (ADR 0016).
        let projector = match cfg.postgres.projection_mode {
            ProjectionMode::Background => Projector::spawn(
                pool.clone(),
                Duration::from_millis(cfg.postgres.projection_interval_ms),
            ),
            ProjectionMode::OnDemand => Projector::on_demand(pool.clone(), Vec::new()),
        };
        let projection = projector.handle();

        let projection_trigger = cfg
            .postgres
            .projection_trigger_secret
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|secret| ProjectionTrigger {
                projection: projection.clone(),
                secret: secret.into(),
            });

        let router = http::router(
            AppState {
                writer: writer.handle(),
                store: LineageStore::new(pool.clone()),
                projection_trigger,
            },
            &cfg.ui.base_path,
            cfg.ui.serve,
        );

        Ok(Self {
            router,
            settler: Settler {
                writer: writer.handle(),
                projection,
            },
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

    /// A handle that flushes and folds on demand.
    pub fn settler(&self) -> Settler {
        self.settler.clone()
    }

    /// Drain buffered events (bounded by `drain_timeout`), stop the projector
    /// after a final fold, and close the pool.
    ///
    /// The writer only drains once every handle is gone, so drop every
    /// [`router`](Self::router) clone (e.g. stop the server) before calling.
    pub async fn shutdown(self, drain_timeout: Duration) {
        // Our own router clone and settler hold writer handles too; drop them
        // first.
        drop(self.router);
        drop(self.settler);
        tracing::info!("draining buffered writer");
        self.writer.shutdown(drain_timeout).await;
        tracing::info!("stopping projection worker");
        self.projector.shutdown().await;
        self.pool.close().await;
    }
}

#[cfg(all(test, feature = "postgres-it"))]
mod on_demand_tests {
    //! On-demand projection end to end: ingest through the router, then fold
    //! through the trigger route or a settler. Needs Docker; run with:
    //!   cargo test -p headwaters --features postgres-it on_demand
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;
    use crate::config::PostgresConfig;
    use crate::http::PROJECTION_TRIGGER_PATH;
    use crate::test_support::{Db, start_postgres};

    const SECRET: &str = "s3cret";

    async fn start(db: &Db) -> Service {
        let cfg = Config {
            postgres: PostgresConfig {
                url: Some(db.url.clone()),
                pool_size: 2,
                projection_mode: ProjectionMode::OnDemand,
                projection_trigger_secret: Some(SECRET.into()),
                ..PostgresConfig::default()
            },
            ..Config::default()
        };
        Service::start(&cfg).await.expect("start service")
    }

    async fn ingest(router: &Router, job: &str) {
        let body = format!(
            r#"{{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z",
                "producer":"test-producer","run":{{"runId":"run-{job}"}},
                "job":{{"namespace":"etl","name":"{job}"}}}}"#
        );
        let resp = router
            .clone()
            .oneshot(
                Request::post("/api/v1/lineage")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    async fn trigger(router: &Router, auth: Option<&str>) -> (StatusCode, String) {
        let mut req = Request::post(PROJECTION_TRIGGER_PATH);
        if let Some(auth) = auth {
            req = req.header("authorization", auth);
        }
        let resp = router
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    async fn jobs(db: &Db) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM jobs")
            .fetch_one(&db.pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn trigger_route_flushes_and_folds() {
        let db = start_postgres().await;
        let service = start(&db).await;
        let router = service.router();

        ingest(&router, "a").await;
        // Past a writer flush interval: with no background task nothing folds.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(jobs(&db).await, 0);

        assert_eq!(trigger(&router, None).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            trigger(&router, Some("Bearer wrong")).await.0,
            StatusCode::UNAUTHORIZED
        );
        let (status, body) = trigger(&router, Some(&format!("Bearer {SECRET}"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, r#"{"applied":1,"caught_up":true}"#);
        assert_eq!(jobs(&db).await, 1);

        drop(router);
        service.shutdown(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn settle_folds_events_still_buffered_by_the_writer() {
        let db = start_postgres().await;
        let service = start(&db).await;
        let router = service.router();
        let settler = service.settler();

        ingest(&router, "a").await;
        ingest(&router, "b").await;
        // Right after ingest the events are still buffered; settle waits for
        // the flush rather than folding an empty log.
        let run = settler
            .settle(Duration::from_secs(5))
            .await
            .expect("settle");
        assert_eq!(
            run,
            ProjectionRun {
                applied: 2,
                caught_up: true
            }
        );
        assert_eq!(jobs(&db).await, 2);

        drop((router, settler));
        service.shutdown(Duration::from_secs(5)).await;
    }
}
