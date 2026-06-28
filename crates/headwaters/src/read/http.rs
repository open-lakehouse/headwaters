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
use crate::proto::headwaters::read::v1 as pb;

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
        // Home-page activity charts: time-bucketed counts off the event log.
        .route("/api/v1/stats/lineage-events", get(stats_lineage_events))
        .route("/api/v1/stats/:asset", get(stats_asset))
        // Tag catalog the UI fetches on load.
        .route("/api/v1/tags", get(list_tags))
        // Tag/PII propagation: the fields reachable downstream from a tag.
        .route("/api/v1/tags/:tag/downstream", get(tag_downstream))
        // Normalize JSON responses so the upstream Marquez web UI (which assumes
        // empty arrays like `tags`/`inputs` are always present) doesn't crash on
        // proto3-JSON's omitted-empty fields. Additive only — a no-op for our own
        // UI's generated client. See `super::marquez_compat`.
        .layer(axum::middleware::map_response(
            super::marquez_compat::normalize,
        ))
        .with_state(store)
}

#[derive(Debug, Deserialize)]
struct StatsParams {
    /// `DAY` (default) | `HOUR` | etc. — passed to `date_trunc`.
    #[serde(default = "default_period")]
    period: String,
    #[serde(default = "default_stats_limit")]
    limit: usize,
}

fn default_period() -> String {
    "day".into()
}

fn default_stats_limit() -> usize {
    30
}

/// `GET /api/v1/stats/lineage-events?period=&limit=` — time-bucketed event counts.
///
/// Marquez returns a bare JSON array of `{date, count}` here (no envelope), so
/// we serialize the response message's `buckets` directly rather than the
/// `StatsResponse` wrapper.
async fn stats_lineage_events(
    State(store): State<LineageStore>,
    Query(p): Query<StatsParams>,
) -> Result<impl IntoResponse, ReadError> {
    let limit = super::resolve_limit(p.limit, default_stats_limit());
    let resp = store.stats_lineage_events(&p.period, limit).await?;
    Ok(Json(resp.buckets))
}

/// `GET /api/v1/stats/:asset?period=&limit=` — first-seen counts for `jobs` or
/// `datasets`, bucketed by period. Bare array, like `stats_lineage_events`.
///
/// The Marquez dashboard also requests `stats/sources` (and may add others); for
/// assets we don't track we return an empty array rather than a 404, so the UI's
/// metrics panel degrades gracefully instead of logging fetch errors.
async fn stats_asset(
    State(store): State<LineageStore>,
    Path(asset): Path<String>,
    Query(p): Query<StatsParams>,
) -> Result<impl IntoResponse, ReadError> {
    let limit = super::resolve_limit(p.limit, default_stats_limit());
    match store.stats_asset(&asset, &p.period, limit).await {
        Ok(resp) => Ok(Json(resp.buckets)),
        Err(ReadError::NotFound(_)) => {
            Ok(Json(Vec::<crate::headwaters::read::v1::StatBucket>::new()))
        }
        Err(e) => Err(e),
    }
}

/// `GET /api/v1/tags` — the tag catalog.
async fn list_tags(State(store): State<LineageStore>) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.tags().await?))
}

/// `GET /api/v1/tags/:tag/downstream` — the dataset fields reachable downstream
/// from anything currently tagged `tag`, via column (then table) lineage.
async fn tag_downstream(
    State(store): State<LineageStore>,
    Path(tag): Path<String>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.tag_downstream(&tag).await?))
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

impl Pagination {
    /// The page size to query, resolved against the shared default/ceiling so the
    /// REST surface clamps identically to Connect: `limit=0` → default,
    /// oversized → [`MAX_LIMIT`](super::MAX_LIMIT). Without this, `?limit=0`
    /// returned an empty page and an arbitrarily large `?limit` could materialize
    /// a whole table.
    fn limit(&self) -> usize {
        super::resolve_limit(self.limit, super::DEFAULT_LIMIT)
    }
}

fn default_limit() -> usize {
    super::DEFAULT_LIMIT
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
    Ok(Json(store.jobs(None, page.limit(), page.offset).await?))
}

async fn list_jobs(
    State(store): State<LineageStore>,
    Path(namespace): Path<String>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(
        store
            .jobs(Some(&namespace), page.limit(), page.offset)
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
    Ok(Json(store.datasets(None, page.limit(), page.offset).await?))
}

async fn list_datasets(
    State(store): State<LineageStore>,
    Path(namespace): Path<String>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(
        store
            .datasets(Some(&namespace), page.limit(), page.offset)
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
    /// `job` | `dataset` (case-insensitive); absent or unrecognized returns both.
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
}

async fn search(
    State(store): State<LineageStore>,
    Query(params): Query<SearchParams>,
) -> Result<impl IntoResponse, ReadError> {
    let kind = params.r#type.as_deref().and_then(parse_entity_kind);
    let limit = super::resolve_limit(params.limit, super::DEFAULT_LIMIT);
    Ok(Json(
        store
            .search(&params.q, limit, kind, params.namespace.as_deref())
            .await?,
    ))
}

/// Map a `?type=` query value (`job` / `dataset`, case-insensitive) to an
/// [`pb::EntityKind`] filter. Returns `None` for absent/unrecognized values, so
/// the search returns both kinds.
fn parse_entity_kind(s: &str) -> Option<pb::EntityKind> {
    match s.to_ascii_lowercase().as_str() {
        "job" => Some(pb::EntityKind::JOB),
        "dataset" => Some(pb::EntityKind::DATASET),
        _ => None,
    }
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
    let graph = store.lineage(&params.node_id, params.depth).await?;
    Ok(Json(lineage_envelope(graph)))
}

/// Wrap a lineage graph in an envelope that *always* carries the `graph` key.
///
/// The proto message omits an empty `graph` from JSON (proto3 drops empty
/// repeated fields), but the web UI's graph layout reads `payload.graph` and
/// crashes (`.map()` of undefined) when it is absent — and the column-lineage
/// view legitimately returns an empty graph (200, not 404). So for the REST
/// surface we serialize the nodes under an unconditional `graph` field. (The
/// Connect surface keeps proto semantics: typed clients default a missing
/// repeated to an empty list.)
fn lineage_envelope(graph: crate::headwaters::read::v1::LineageGraph) -> serde_json::Value {
    serde_json::json!({ "graph": graph.graph })
}

async fn get_dataset_versions(
    State(store): State<LineageStore>,
    Path((namespace, dataset)): Path<(String, String)>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(
        store
            .dataset_versions(&namespace, &dataset, page.limit(), page.offset)
            .await?,
    ))
}

async fn list_events(
    State(store): State<LineageStore>,
    Query(page): Query<Pagination>,
) -> Result<impl IntoResponse, ReadError> {
    Ok(Json(store.events(page.limit(), page.offset).await?))
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
    let graph = store.column_lineage(&params.node_id).await?;
    Ok(Json(lineage_envelope(graph)))
}
