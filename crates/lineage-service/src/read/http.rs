//! Marquez-compatible read endpoints, served under `/api/v1`.
//!
//! These are the GET routes the Marquez web UI calls to populate the namespace /
//! job / dataset browse views, the lineage graph, the events feed, and the
//! job/dataset detail drawers. They are backed by [`LineageStore`], which
//! reconstructs Marquez's model from the events table. Runs, dataset versions,
//! and column lineage are reconstructed from the event log; tags and
//! time-bucketed metrics remain stubbed (empty but non-404) — see [`super`].

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use serde::Deserialize;

use super::{LineageStore, ReadError};

/// Build the read router. Routes carry the full `/api/v1` prefix (the Marquez
/// web client prefixes every call with its `__API_URL__`, which we configure to
/// end in `/api/v1`). The router is `merge`d into the service's top-level router
/// rather than nested, so its `GET /api/v1/lineage` coexists with the ingest
/// side's `POST /api/v1/lineage`.
pub fn router(store: LineageStore) -> Router {
    // NOTE: axum 0.7 path captures use the `:param` syntax (`{param}` is a
    // literal segment here — that changed in axum 0.8). Keep these as `:param`
    // until the crate moves to axum 0.8.
    Router::new()
        .route("/api/v1/namespaces", get(list_namespaces))
        .route("/api/v1/jobs", get(list_all_jobs))
        .route("/api/v1/datasets", get(list_all_datasets))
        .route("/api/v1/namespaces/:namespace/jobs", get(list_jobs))
        .route("/api/v1/namespaces/:namespace/jobs/:job", get(get_job))
        .route(
            "/api/v1/namespaces/:namespace/jobs/:job/runs",
            get(get_job_runs),
        )
        .route("/api/v1/namespaces/:namespace/datasets", get(list_datasets))
        .route(
            "/api/v1/namespaces/:namespace/datasets/:dataset",
            get(get_dataset),
        )
        .route(
            "/api/v1/namespaces/:namespace/datasets/:dataset/versions",
            get(get_dataset_versions),
        )
        .route("/api/v1/search", get(search))
        .route("/api/v1/lineage", get(lineage))
        // Events page: a paginated scan of the raw OpenLineage event log.
        .route("/api/v1/events/lineage", get(list_events))
        // Run-detail facets tab.
        .route("/api/v1/jobs/runs/:run_id/facets", get(get_run_facets))
        // Dataset column-lineage view, served from the latest stored
        // column-lineage facet of the addressed dataset (empty graph, not
        // 404, when there is none).
        .route("/api/v1/column-lineage", get(column_lineage))
        // Home-page activity charts. We don't compute time-bucketed metrics, so
        // these return an empty series — the charts render empty rather than
        // erroring on a 404. The UI expects a JSON array for each.
        .route("/api/v1/stats/lineage-events", get(empty_stats))
        .route("/api/v1/stats/:asset", get(empty_stats))
        // Tags: we don't track tags, but the UI fetches the catalog on load and
        // expects a `{ "tags": [] }` envelope. Empty keeps the page from 404ing.
        .route("/api/v1/tags", get(empty_tags))
        .with_state(store)
}

/// Empty metric series for the unimplemented `/stats/*` endpoints (see router).
async fn empty_stats() -> impl IntoResponse {
    Json(serde_json::json!([]))
}

/// Empty tag catalog for the unimplemented `/tags` endpoint (see router).
async fn empty_tags() -> impl IntoResponse {
    Json(serde_json::json!({ "tags": [] }))
}

/// Map a [`ReadError`] onto an HTTP response: 404 for not-found, 500 otherwise.
impl IntoResponse for ReadError {
    fn into_response(self) -> axum::response::Response {
        let status = match self {
            ReadError::NotFound(_) => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        tracing::warn!(error = %self, "lineage read error");
        (
            status,
            Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response()
    }
}

#[derive(Debug, Deserialize)]
struct Pagination {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

fn default_limit() -> usize {
    100
}

async fn list_namespaces(
    State(store): State<LineageStore>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.namespaces().await?))
}

/// Global `GET /api/v1/jobs` — jobs across all namespaces (the UI's main jobs view).
async fn list_all_jobs(
    State(store): State<LineageStore>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.jobs(None, page.limit, page.offset).await?))
}

async fn list_jobs(
    State(store): State<LineageStore>,
    Path(namespace): Path<String>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(
        store
            .jobs(Some(&namespace), page.limit, page.offset)
            .await?,
    ))
}

async fn get_job(
    State(store): State<LineageStore>,
    Path((namespace, job)): Path<(String, String)>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.job(&namespace, &job).await?))
}

async fn get_job_runs(
    State(store): State<LineageStore>,
    Path((namespace, job)): Path<(String, String)>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.job_runs(&namespace, &job).await?))
}

/// Global `GET /api/v1/datasets` — datasets across all namespaces.
async fn list_all_datasets(
    State(store): State<LineageStore>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.datasets(None, page.limit, page.offset).await?))
}

async fn list_datasets(
    State(store): State<LineageStore>,
    Path(namespace): Path<String>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(
        store
            .datasets(Some(&namespace), page.limit, page.offset)
            .await?,
    ))
}

async fn get_dataset(
    State(store): State<LineageStore>,
    Path((namespace, dataset)): Path<(String, String)>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.dataset(&namespace, &dataset).await?))
}

#[derive(Debug, Deserialize)]
struct SearchParams {
    #[serde(default)]
    q: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

async fn search(
    State(store): State<LineageStore>,
    Query(params): Query<SearchParams>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.search(&params.q, params.limit).await?))
}

#[derive(Debug, Deserialize)]
struct LineageParams {
    #[serde(rename = "nodeId")]
    node_id: String,
    #[serde(default = "default_depth")]
    depth: usize,
}

fn default_depth() -> usize {
    20
}

async fn lineage(
    State(store): State<LineageStore>,
    Query(params): Query<LineageParams>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.lineage(&params.node_id, params.depth).await?))
}

async fn get_dataset_versions(
    State(store): State<LineageStore>,
    Path((namespace, dataset)): Path<(String, String)>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(
        store
            .dataset_versions(&namespace, &dataset, page.limit, page.offset)
            .await?,
    ))
}

async fn list_events(
    State(store): State<LineageStore>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.events(page.limit, page.offset).await?))
}

async fn get_run_facets(
    State(store): State<LineageStore>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.run_facets(&run_id).await?))
}

#[derive(Debug, Deserialize)]
struct ColumnLineageParams {
    #[serde(rename = "nodeId", default)]
    node_id: String,
}

async fn column_lineage(
    State(store): State<LineageStore>,
    Query(params): Query<ColumnLineageParams>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.column_lineage(&params.node_id).await?))
}
