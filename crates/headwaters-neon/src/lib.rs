//! headwaters as a Node-API addon.
//!
//! The Neon Function's JS handler (`function/index.ts`) calls [`start`] once,
//! forwards every fetch `Request` through [`handle`], and calls [`shutdown`]
//! on SIGINT. Requests never touch a socket: [`handle`] drives the in-process
//! axum router with `oneshot`. Projection runs on demand: the handler calls
//! [`settle`] after ingest and from a cron trigger (ADR 0016).

use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use headwaters::config::{Config, ProjectionMode};
use headwaters::embed::{Service, Settler};
use http_body_util::BodyExt;
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;
use tower::ServiceExt;

/// Neon sends SIGINT and hard-kills 5 s later; leave headroom for the
/// projector's final fold and pool close after the writer drain.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(3);

/// The running service. `None` before [`start`] and after [`shutdown`].
static SERVICE: Mutex<Option<Service>> = Mutex::new(None);
/// The router, cloned out of [`SERVICE`] so requests don't take the lock.
static ROUTER: Mutex<Option<Router>> = Mutex::new(None);
/// The settler, cloned out of [`SERVICE`] for the same reason.
static SETTLER: Mutex<Option<Settler>> = Mutex::new(None);
static TRACING: OnceLock<()> = OnceLock::new();

#[napi(object)]
pub struct Header {
    pub name: String,
    pub value: String,
}

#[napi(object)]
pub struct JsRequest {
    pub method: String,
    /// Path and query, e.g. `/api/v1/namespaces?limit=10`.
    pub url: String,
    pub headers: Vec<Header>,
    pub body: Buffer,
}

#[napi(object)]
pub struct JsResponse {
    pub status: u16,
    pub headers: Vec<Header>,
    pub body: Buffer,
}

/// What a [`settle`] call folded. Fields reach JS as `applied` / `caughtUp`.
#[napi(object)]
pub struct JsProjectionRun {
    pub applied: u32,
    pub caught_up: bool,
}

fn to_napi(e: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(e.to_string())
}

/// Load config, connect, verify the schema, and spawn the background workers.
///
/// Config comes from the same layers as the standalone binary (`HEADWATERS__*`,
/// `DATABASE_URL`). When no schema-check DSN is configured, Neon's direct
/// `DATABASE_URL_UNPOOLED` is used for it, since `DATABASE_URL` is pooled.
/// Projection defaults to on demand, since isolates don't keep background
/// work alive between requests; `HEADWATERS__POSTGRES__PROJECTION_MODE`
/// overrides that. The bundled UI is never served. Idempotent: a second call
/// is a no-op.
#[napi]
pub async fn start() -> napi::Result<()> {
    TRACING.get_or_init(|| {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "headwaters=info".into());
        let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
    });
    if ROUTER.lock().map_err(to_napi)?.is_some() {
        return Ok(());
    }

    let mut cfg = Config::load(None::<&str>).map_err(to_napi)?;
    if cfg.postgres.schema_check_url.is_none() {
        cfg.postgres.schema_check_url = std::env::var("DATABASE_URL_UNPOOLED").ok();
    }
    if std::env::var_os("HEADWATERS__POSTGRES__PROJECTION_MODE").is_none() {
        cfg.postgres.projection_mode = ProjectionMode::OnDemand;
    }
    cfg.ui.serve = false;

    let service = Service::start(&cfg)
        .await
        .map_err(|e| to_napi(format!("{e:#}")))?;
    *ROUTER.lock().map_err(to_napi)? = Some(service.router());
    *SETTLER.lock().map_err(to_napi)? = Some(service.settler());
    *SERVICE.lock().map_err(to_napi)? = Some(service);
    tracing::info!("headwaters started");
    Ok(())
}

/// Serve one request through the router and buffer the whole response.
#[napi]
pub async fn handle(req: JsRequest) -> napi::Result<JsResponse> {
    let router = ROUTER
        .lock()
        .map_err(to_napi)?
        .clone()
        .ok_or_else(|| to_napi("headwaters is not started"))?;

    let mut builder = axum::http::Request::builder()
        .method(req.method.as_str())
        .uri(req.url.as_str());
    for h in &req.headers {
        builder = builder.header(h.name.as_str(), h.value.as_str());
    }
    let request = builder
        .body(Body::from(req.body.to_vec()))
        .map_err(to_napi)?;

    let response = router.oneshot(request).await.map_err(to_napi)?;
    let (parts, body) = response.into_parts();
    let body = body.collect().await.map_err(to_napi)?.to_bytes();

    let headers = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            Some(Header {
                name: name.to_string(),
                value: value.to_str().ok()?.to_string(),
            })
        })
        .collect();
    Ok(JsResponse {
        status: parts.status.as_u16(),
        headers,
        body: body.to_vec().into(),
    })
}

/// Wait for buffered events to be written, then fold the event log for up to
/// `budget_ms`.
#[napi]
pub async fn settle(budget_ms: u32) -> napi::Result<JsProjectionRun> {
    let settler = SETTLER
        .lock()
        .map_err(to_napi)?
        .clone()
        .ok_or_else(|| to_napi("headwaters is not started"))?;
    let run = settler
        .settle(Duration::from_millis(budget_ms.into()))
        .await
        .map_err(to_napi)?;
    Ok(JsProjectionRun {
        applied: u32::try_from(run.applied).unwrap_or(u32::MAX),
        caught_up: run.caught_up,
    })
}

/// Stop accepting requests, flush buffered events, and stop the workers.
#[napi]
pub async fn shutdown() -> napi::Result<()> {
    ROUTER.lock().map_err(to_napi)?.take();
    SETTLER.lock().map_err(to_napi)?.take();
    let service = SERVICE.lock().map_err(to_napi)?.take();
    if let Some(service) = service {
        service.shutdown(DRAIN_TIMEOUT).await;
    }
    Ok(())
}
