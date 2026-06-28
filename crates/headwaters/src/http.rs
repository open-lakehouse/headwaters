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

use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{Html, IntoResponse, Json};
use axum::routing::{get, post};
use serde::Serialize;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
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

/// Build the service router: `/health`, `/version`, the OpenLineage ingest
/// endpoints, the Marquez-compatible read API under `/api/v1`, and the
/// ConnectRPC read service under [`CONNECT_PREFIX`].
///
/// Anything not matched by an API route falls back to the bundled single-page
/// app in [`UI_DIR`]: real files come off disk, and any other path (deep links
/// like `/jobs`) falls back to `index.html` so the SPA's client-side router
/// takes over. API routes are matched first, so they are never shadowed. When
/// [`UI_DIR`] has no bundle (local API-only runs), those paths 404.
///
/// When `base_path` is non-empty (e.g. `/lineage`) the *entire* surface — UI,
/// REST API, and ConnectRPC — is mounted under that prefix, so the service can
/// sit behind a gateway at a sub-path. See [`mount_under_base`]. The bundle's
/// asset URLs are relative (Vite `base: "./"`) and the served `index.html` is
/// rewritten to carry the prefix (see [`serve_index`]), so one image works at
/// any prefix without a rebuild.
///
/// A permissive [`CorsLayer`] is applied because a separately-hosted web UI
/// (e.g. the Marquez reference UI, or the Vite dev server) calls these endpoints
/// directly from another origin.
pub fn router(state: AppState, base_path: &str) -> Router {
    router_in(state, UI_DIR, base_path)
}

/// [`router`], with the SPA directory injected — the seam is so tests can point
/// at a fixture bundle instead of the hardcoded [`UI_DIR`], and exercise the
/// base-path mounting with a known prefix.
fn router_in(state: AppState, ui_dir: impl AsRef<Path>, base_path: &str) -> Router {
    let ui_dir = ui_dir.as_ref().to_path_buf();
    let read_routes = read::http::router(state.store.clone());

    // The read API also speaks ConnectRPC, served on this same listener so the
    // web UI can use generated typed clients. `LineageStore` implements the
    // generated `ReadService` trait (see `read::connect`), delegating to the same
    // store the REST handlers use — one model, two surfaces.
    let connect_router =
        ReadServiceExt::register(Arc::new(state.store.clone()), connectrpc::Router::new());

    let ingest_routes = Router::new()
        .route("/health", get(|| async { "OK" }))
        // The crate version, which release-plz bumps and which the Docker image
        // is tagged with — so this is the single way to confirm, against a
        // running service, exactly which release (binary + bundled UI) is live.
        .route("/version", get(version))
        .route("/api/v1/lineage", post(ingest_event))
        .route("/api/v1/lineage/batch", post(ingest_batch))
        .with_state(state);

    // The SPA entry (`index.html`) is always served through `serve_index` so the
    // base path is injected — never straight off disk. `serve_index` ignores the
    // request, so a zero-arg handler closure suffices; this factory stamps out one
    // per route that needs it (the explicit index routes and the deep-link
    // fallback) without juggling clones inline.
    let base_path = base_path.to_string();
    let index_handler = || {
        let ui_dir = ui_dir.clone();
        let base_path = base_path.clone();
        get(move || {
            let ui_dir = ui_dir.clone();
            let base_path = base_path.clone();
            async move { serve_index(&ui_dir, &base_path).await }
        })
    };

    // ServeDir serves only the real, hashed asset files. Directory-index appending
    // is OFF so it never serves `index.html` itself off disk (that must go through
    // `serve_index` for the base-path injection). Its own fallback handles deep
    // links: any path that isn't a real file -> the templated SPA entry, so the
    // client-side router takes over (and a missing bundle -> 404).
    let serve_assets = ServeDir::new(&ui_dir)
        .append_index_html_on_directories(false)
        .fallback(index_handler());

    let app = ingest_routes
        .merge(read_routes)
        // The SPA entry, templated, at the root and its explicit file name —
        // ServeDir would otherwise serve these straight off disk, un-injected.
        .route("/", index_handler())
        .route("/index.html", index_handler())
        // Mount the Connect dispatcher under its own path prefix. `route_service`
        // (not `nest_service`) keeps the full URI intact, which the dispatcher
        // needs since it routes on the fully-qualified `service/method` path.
        // axum 0.7 catch-all syntax is `/*param` (`{*param}` is 0.8); keep this in
        // step with the `:param` captures in `read::http` until the crate moves.
        .route_service(
            &format!("{CONNECT_PREFIX}/*rest"),
            connect_router.into_axum_service(),
        )
        // Everything else: real asset files off disk, else the templated SPA entry.
        .fallback_service(serve_assets);

    mount_under_base(app, &base_path)
        // Per-request tracing (method, path, status, latency) for operability;
        // verbosity is controlled by the `RUST_LOG`/`tower_http` env filter.
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
}

/// Mount `app` under `base_path`, or return it unchanged when the prefix is
/// empty (serve at root — today's behavior, byte-for-byte).
///
/// Rather than `Router::nest` — whose trailing-slash handling at the mount root
/// is fiddly (`/{prefix}` vs `/{prefix}/` resolve inconsistently against the
/// inner `/` route) — this strips the prefix from the request path *before* the
/// inner router routes, then delegates to the unchanged inner `app`. The strip
/// runs as a layer wrapping the whole router **as a service** (via
/// `ServiceBuilder`), not `Router::layer` — the latter runs only after a route is
/// matched, too late to influence routing. The inner router (including the
/// ConnectRPC dispatcher, which routes on the fully-qualified service path) then
/// sees exactly the path it expects, and both `/{prefix}` and `/{prefix}/` map
/// cleanly to `/`. Requests outside the prefix get a 404.
fn mount_under_base(app: Router, base_path: &str) -> Router {
    if base_path.is_empty() {
        return app;
    }
    let prefix = base_path.to_string();
    let stripped = tower::ServiceBuilder::new()
        .layer(axum::middleware::from_fn(
            move |mut req: axum::extract::Request, next: axum::middleware::Next| {
                let prefix = prefix.clone();
                async move {
                    let path = req.uri().path();
                    // Strip the prefix; `/{prefix}` and `/{prefix}/` -> `/`.
                    let new_path = match path.strip_prefix(&prefix) {
                        Some("") => Some("/".to_string()),
                        Some(rest) if rest.starts_with('/') => Some(rest.to_string()),
                        // A path that merely *starts* with the prefix as a
                        // substring (e.g. `/{prefix}foo`) is not under it -> 404.
                        _ => None,
                    };
                    match new_path {
                        Some(new_path) => {
                            rewrite_path(&mut req, &new_path);
                            next.run(req).await
                        }
                        None => StatusCode::NOT_FOUND.into_response(),
                    }
                }
            },
        ))
        .service(app);
    Router::new().fallback_service(stripped)
}

/// Replace the path of `req`'s URI in place, preserving the query string.
fn rewrite_path(req: &mut axum::extract::Request, new_path: &str) {
    let uri = req.uri();
    let path_and_query = match uri.query() {
        Some(q) => format!("{new_path}?{q}"),
        None => new_path.to_string(),
    };
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(
        path_and_query
            .parse()
            .expect("rewritten path-and-query is valid"),
    );
    if let Ok(new_uri) = axum::http::Uri::from_parts(parts) {
        *req.uri_mut() = new_uri;
    }
}

/// The global the SPA reads on boot to learn the prefix it is served under (see
/// `node/app/src/main.tsx`). Empty string = served at root.
const BASE_PATH_GLOBAL: &str = "__HEADWATERS_BASE_PATH__";

/// Serve the SPA entry point (`index.html`) with the active base path injected,
/// for the catch-all fallback (deep links like `/jobs`, and the prefix root).
///
/// Two things are injected just inside `<head>`:
///   - `<base href="{base_path}/">` so the bundle's *relative* asset URLs
///     (Vite is built with `base: "./"`) resolve under the prefix;
///   - `<script>window.__HEADWATERS_BASE_PATH__ = "{base_path}"</script>` so the
///     client-side router and the ConnectRPC transport pick up the prefix.
///
/// When `base_path` is empty the base href is `/` and the global is `""` — the
/// SPA behaves exactly as a root deployment. A missing bundle (API-only runs)
/// returns 404, leaving the API serving and the server up.
async fn serve_index(ui_dir: &Path, base_path: &str) -> axum::response::Response {
    let html = match tokio::fs::read_to_string(ui_dir.join("index.html")).await {
        Ok(html) => html,
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };

    // `<base href>` needs a trailing slash so relative URLs resolve as
    // `/{base_path}/assets/...`; root is just `/`.
    let base_href = if base_path.is_empty() {
        "/".to_string()
    } else {
        format!("{base_path}/")
    };
    let injection = format!(
        "<base href=\"{base_href}\">\
         <script>window.{BASE_PATH_GLOBAL} = \"{base_path}\";</script>"
    );

    // Inject right after the opening <head>; if (unexpectedly) absent, prepend so
    // the tags still take effect.
    let rewritten = match html.find("<head>") {
        Some(idx) => {
            let cut = idx + "<head>".len();
            format!("{}{injection}{}", &html[..cut], &html[cut..])
        }
        None => format!("{injection}{html}"),
    };

    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        Html(rewritten),
    )
        .into_response()
}

#[derive(Serialize)]
struct VersionBody {
    version: &'static str,
}

/// `GET /version` — the running `headwaters` crate version, as
/// `{"version": "x.y.z"}`. release-plz bumps this version and tags the Docker
/// image with it, so a deployed instance reports exactly the release it is.
async fn version() -> Json<VersionBody> {
    Json(VersionBody {
        version: env!("CARGO_PKG_VERSION"),
    })
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
        // A realistic Vite-built entry: a <head> for the base-path injection and
        // a *relative* asset URL (Vite `base: "./"`) that <base href> resolves.
        std::fs::write(
            dir.path().join("index.html"),
            "<!doctype html><html><head><title>spa</title></head>\
             <body><script src=\"assets/app.js\"></script></body></html>",
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
        let app = router_in(test_state(), dir.path(), "");
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp).await, "OK");
    }

    #[tokio::test]
    async fn version_reports_the_crate_version() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), "");
        let resp = app
            .oneshot(Request::get("/version").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            body_string(resp).await,
            format!(r#"{{"version":"{}"}}"#, env!("CARGO_PKG_VERSION"))
        );
    }

    #[tokio::test]
    async fn deep_links_fall_back_to_index_html() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), "");
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
        let app = router_in(test_state(), dir.path(), "");
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
        let app = router_in(test_state(), dir.path(), "");
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
        let app = router_in(test_state(), dir.path(), "");
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

    #[tokio::test]
    async fn index_html_carries_base_href_and_global_at_root() {
        // Even at root (empty prefix) the entry is rewritten: base href `/`, the
        // global as the empty string. The SPA then behaves as a root deployment.
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), "");
        let resp = app
            .oneshot(Request::get("/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp).await;
        assert!(body.contains(r#"<base href="/">"#), "body: {body}");
        assert!(
            body.contains(r#"window.__HEADWATERS_BASE_PATH__ = "";"#),
            "body: {body}"
        );
    }

    // Base-path mounting: with a prefix configured, the whole surface lives under
    // it. Root paths 404, prefixed paths behave as the unprefixed ones did, and
    // the served index.html carries the prefix.
    const PREFIX: &str = "/lineage";

    #[tokio::test]
    async fn prefixed_health_works_and_unprefixed_404s() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), PREFIX);
        let ok = app
            .clone()
            .oneshot(Request::get("/lineage/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        assert_eq!(body_string(ok).await, "OK");

        // The root path no longer exists when a prefix is set.
        let gone = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(gone.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn prefixed_deep_link_serves_index_with_injected_prefix() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), PREFIX);
        let resp = app
            .oneshot(
                Request::get("/lineage/jobs/deep/link")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_string(resp).await;
        assert!(body.contains(r#"<base href="/lineage/">"#), "body: {body}");
        assert!(
            body.contains(r#"window.__HEADWATERS_BASE_PATH__ = "/lineage";"#),
            "body: {body}"
        );
        assert!(body.contains("<title>spa</title>"), "body: {body}");
    }

    #[tokio::test]
    async fn prefixed_assets_are_served_from_disk() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), PREFIX);
        let resp = app
            .oneshot(
                Request::get("/lineage/assets/app.js")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(body_string(resp).await, "console.log(1)");
    }

    #[tokio::test]
    async fn prefixed_connect_rpc_reaches_the_dispatcher() {
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), PREFIX);
        let resp = app
            .oneshot(
                Request::get("/lineage/headwaters.read.v1.ReadService/ListNamespaces")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Reached Connect (not the SPA fallback), so not the bundle HTML.
        assert!(!body_string(resp).await.contains("<title>spa</title>"));
    }

    #[tokio::test]
    async fn prefix_root_with_and_without_trailing_slash_both_serve_index() {
        // Both `/lineage` and `/lineage/` map to the SPA entry (the injected
        // `<base href>` is absolute, so a missing trailing slash still resolves
        // assets correctly — no redirect needed).
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), PREFIX);
        for path in ["/lineage", "/lineage/"] {
            let resp = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "path {path}");
            let body = body_string(resp).await;
            assert!(
                body.contains(r#"<base href="/lineage/">"#),
                "path {path} body: {body}"
            );
        }
    }

    #[tokio::test]
    async fn paths_outside_the_prefix_404() {
        // A path that only shares the prefix as a substring is not under it.
        let dir = write_bundle();
        let app = router_in(test_state(), dir.path(), PREFIX);
        let resp = app
            .oneshot(Request::get("/lineagex/jobs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
