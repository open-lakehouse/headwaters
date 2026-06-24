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

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use serde::Serialize;
use tower_http::cors::CorsLayer;

use crate::ingest::{convert_batch, convert_event};
use crate::read::{self, LineageStore};
use crate::writer::buffered::BufferedWriterHandle;

/// Shared handler state: a handle onto the buffered writer.
#[derive(Clone)]
pub struct AppState {
    pub writer: BufferedWriterHandle,
    /// Read-only handle onto the events table for the Marquez-compatible API.
    pub store: LineageStore,
}

/// Build the service router: `/health`, the OpenLineage ingest endpoints, and
/// the Marquez-compatible read API under `/api/v1`. The read routes are merged
/// in with their own [`LineageStore`] state.
///
/// A permissive [`CorsLayer`] is applied because the Marquez web UI is served
/// from a different origin and calls these endpoints directly from the browser.
pub fn router(state: AppState) -> Router {
    let read_routes = read::http::router(state.store.clone());

    let ingest_routes = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/api/v1/lineage", post(ingest_event))
        .route("/api/v1/lineage/batch", post(ingest_batch))
        .with_state(state);

    ingest_routes
        .merge(read_routes)
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
