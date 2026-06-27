//! HTTP ingestion surface.
//!
//! Accepts OpenLineage JSON on the spec-conventional endpoints and hands every
//! parsed event to the [`BufferedWriter`](crate::writer::buffered) via a
//! cloneable handle. Handlers do not block on lakehouse writes — they return
//! `202 Accepted` once an event is parsed and buffered.
//!
//! Replaces the Go ingest service's REST handlers
//! (`services/lineage/internal/ingest/handler.go`); the batch response shape is
//! preserved.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use serde::Serialize;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

use crate::connect_gen::headwaters::read::v1::ReadServiceExt;
use crate::ingest::{convert_batch, convert_event};
use crate::read::{self, LineageStore};
use crate::writer::buffered::BufferedWriterHandle;

/// Shared handler state: a handle onto the buffered writer.
#[derive(Clone)]
pub struct AppState {
    /// Enqueue handle onto the buffered writer for the ingest endpoints.
    pub writer: BufferedWriterHandle,
    /// Read-only handle onto the events table for the Marquez-compatible API.
    pub store: LineageStore,
}

/// The ConnectRPC service path prefix. Every generated RPC method POSTs to
/// `/headwaters.read.v1.ReadService/<Method>`, so the dispatcher is mounted at
/// this prefix (rather than as a global fallback) to leave the fallback free for
/// the SPA. The dispatcher matches on the *full* request path, so this is wired
/// with `route_service` (which preserves the URI) and not `nest_service` (which
/// would strip the prefix and break dispatch).
const CONNECT_PREFIX: &str = "/headwaters.read.v1.ReadService";

/// Directory the bundled single-page app is served from, relative to the
/// process working directory. The Docker image places the built bundle here (see
/// `Dockerfile`); when it's absent — e.g. local API-only runs where the UI is
/// served by the Vite dev server instead — these paths simply 404.
const UI_DIR: &str = "web";

/// Build the service router: `/health`, the OpenLineage ingest endpoints, the
/// Marquez-compatible read API under `/api/v1`, and the ConnectRPC read service
/// under [`CONNECT_PREFIX`].
///
/// Anything not matched by an API route falls back to the bundled single-page
/// app in [`UI_DIR`]: real files come off disk, and any other path (deep links
/// like `/jobs`) falls back to `index.html` so the SPA's client-side router
/// takes over. API routes are matched first, so they are never shadowed. When
/// [`UI_DIR`] has no bundle (local API-only runs), those paths 404.
///
/// A permissive [`CorsLayer`] is applied because a separately-hosted web UI
/// (e.g. the Marquez reference UI, or the Vite dev server) calls these endpoints
/// directly from another origin.
pub fn router(state: AppState) -> Router {
    router_in(state, UI_DIR)
}

/// [`router`], with the SPA directory injected — the only seam is so tests can
/// point at a fixture bundle instead of the hardcoded [`UI_DIR`].
fn router_in(state: AppState, ui_dir: impl AsRef<std::path::Path>) -> Router {
    let ui_dir = ui_dir.as_ref();
    let read_routes = read::http::router(state.store.clone());

    // The read API also speaks ConnectRPC, served on this same listener so the
    // web UI can use generated typed clients. `LineageStore` implements the
    // generated `ReadService` trait (see `read::connect`), delegating to the same
    // store the REST handlers use — one model, two surfaces.
    let connect_router =
        ReadServiceExt::register(Arc::new(state.store.clone()), connectrpc::Router::new());

    let ingest_routes = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/api/v1/lineage", post(ingest_event))
        .route("/api/v1/lineage/batch", post(ingest_batch))
        .with_state(state);

    // ServeDir serves real assets; unmatched paths fall back to index.html so the
    // client-side router handles deep links (and missing bundle -> 404).
    let serve_ui = ServeDir::new(ui_dir).fallback(ServeFile::new(ui_dir.join("index.html")));

    ingest_routes
        .merge(read_routes)
        // Mount the Connect dispatcher under its own path prefix. `route_service`
        // (not `nest_service`) keeps the full URI intact, which the dispatcher
        // needs since it routes on the fully-qualified `service/method` path.
        // axum 0.7 catch-all syntax is `/*param` (`{*param}` is 0.8); keep this in
        // step with the `:param` captures in `read::http` until the crate moves.
        .route_service(
            &format!("{CONNECT_PREFIX}/*rest"),
            connect_router.into_axum_service(),
        )
        // Everything else: the bundled SPA.
        .fallback_service(serve_ui)
        // Per-request tracing (method, path, status, latency) for operability;
        // verbosity is controlled by the `RUST_LOG`/`tower_http` env filter.
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorBody { error: msg.into() }),
    )
}

#[derive(Serialize)]
struct AcceptedBody {
    status: &'static str,
}

/// `POST /api/v1/lineage` — one OpenLineage event.
async fn ingest_event(State(state): State<AppState>, body: axum::body::Bytes) -> impl IntoResponse {
    let event = match convert_event(&body) {
        Ok(ev) => ev,
        Err(e) => return bad_request(e.to_string()).into_response(),
    };
    if state.writer.enqueue(event).await.is_err() {
        return (StatusCode::SERVICE_UNAVAILABLE, "writer unavailable").into_response();
    }
    (
        StatusCode::ACCEPTED,
        Json(AcceptedBody { status: "accepted" }),
    )
        .into_response()
}

#[derive(Serialize)]
struct BatchSummary {
    received: usize,
    successful: usize,
    failed: usize,
}

#[derive(Serialize)]
struct FailedEvent {
    index: usize,
    reason: String,
    retriable: bool,
}

#[derive(Serialize)]
struct BatchResponse {
    status: &'static str,
    summary: BatchSummary,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    failed_events: Vec<FailedEvent>,
}

/// `POST /api/v1/lineage/batch` — a JSON array of OpenLineage events. Per-event
/// parse failures are reported in the response rather than failing the request;
/// only a non-array body is a 400.
async fn ingest_batch(State(state): State<AppState>, body: axum::body::Bytes) -> impl IntoResponse {
    let outcome = match convert_batch(&body) {
        Ok(o) => o,
        Err(e) => return bad_request(e.to_string()).into_response(),
    };

    let received = outcome.received;
    let failed = outcome.failures.len();
    let mut successful = 0;

    for event in outcome.events {
        if state.writer.enqueue(event).await.is_err() {
            return (StatusCode::SERVICE_UNAVAILABLE, "writer unavailable").into_response();
        }
        successful += 1;
    }

    let failed_events = outcome
        .failures
        .into_iter()
        .map(|f| FailedEvent {
            index: f.index,
            reason: f.reason,
            retriable: false,
        })
        .collect();

    let status = if failed > 0 {
        "partial_success"
    } else {
        "success"
    };

    (
        StatusCode::ACCEPTED,
        Json(BatchResponse {
            status,
            summary: BatchSummary {
                received,
                successful,
                failed,
            },
            failed_events,
        }),
    )
        .into_response()
}

// SPA static-serving wiring tests. These exercise the router's fallback shape
// (API routes win; everything else serves the bundle, deep links -> index.html)
// without a live Postgres: the store wraps a *lazy* pool and none of the routes
// under test issue a query.
#[cfg(test)]
mod serve_ui_tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use sqlx::postgres::PgPoolOptions;
    use tower::ServiceExt; // for `oneshot`

    use super::*;
    use crate::writer::buffered::{BufferedWriter, BufferedWriterConfig};

    fn test_state() -> AppState {
        // A lazy pool never connects until first query; the routes exercised here
        // (`/health`, the SPA fallback) never query, so no Postgres is needed.
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/headwaters_test")
            .expect("lazy pool");
        let writer = BufferedWriter::spawn(Vec::new(), BufferedWriterConfig::default());
        AppState {
            writer: writer.handle(),
            store: LineageStore::new(pool),
        }
    }

    fn write_bundle() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("index.html"),
            "<!doctype html><title>spa</title>",
        )
        .unwrap();
        std::fs::create_dir(dir.path().join("assets")).unwrap();
        std::fs::write(dir.path().join("assets").join("app.js"), "console.log(1)").unwrap();
        dir
    }

    async fn body_string(resp: axum::response::Response) -> String {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn api_routes_are_not_shadowed_by_the_spa_fallback() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path());
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp).await, "OK");
    }

    #[tokio::test]
    async fn deep_links_fall_back_to_index_html() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path());
        // A client-side route with no matching file -> index.html, 200.
        let resp = app
            .oneshot(
                Request::get("/jobs/some/deep/link")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(body_string(resp).await.contains("<title>spa</title>"));
    }

    #[tokio::test]
    async fn real_assets_are_served_from_disk() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path());
        let resp = app
            .oneshot(Request::get("/assets/app.js").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp).await, "console.log(1)");
    }

    #[tokio::test]
    async fn a_missing_bundle_404s_instead_of_serving_the_spa() {
        // API-only runs (no bundle copied in): non-API paths just 404, the
        // server stays up.
        let dir = tempfile::tempdir().expect("tempdir"); // empty: no index.html
        let app = router_in(test_state(), dir.path());
        let resp = app
            .oneshot(Request::get("/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn connect_rpc_prefix_still_routes_to_the_dispatcher() {
        // The Connect path prefix reaches the dispatcher rather than the SPA
        // fallback (a bare GET is rejected by Connect, not served index.html).
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path());
        let resp = app
            .oneshot(
                Request::get("/headwaters.read.v1.ReadService/ListNamespaces")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Whatever Connect returns for a malformed GET, it must not be the SPA.
        assert!(!body_string(resp).await.contains("<title>spa</title>"));
    }
}
