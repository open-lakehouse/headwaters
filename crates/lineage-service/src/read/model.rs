//! Serde structs mirroring the subset of the Marquez REST API the web UI needs
//! to render the namespace/job/dataset browse views and the lineage graph.
//!
//! Field names and envelope keys are taken from the Marquez web client
//! (`web/src/store/requests/*` and `web/src/types/*`). Everything serializes as
//! camelCase to match. Fields we cannot derive from a raw OpenLineage event log
//! (runs, versions, tags, facets, column lineage, metrics) are emitted as empty
//! collections / nulls — the graph views tolerate that and we keep the contract
//! shape intact.

use serde::Serialize;

/// `{ namespace, name }` — the identity object the UI uses for both jobs and
/// datasets.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EntityId {
    pub namespace: String,
    pub name: String,
}

// --- /namespaces ---

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Namespace {
    pub name: String,
    pub created_at: String,
    pub updated_at: String,
    pub owner_name: Option<String>,
    pub description: Option<String>,
    pub is_hidden: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Namespaces {
    pub namespaces: Vec<Namespace>,
}

// --- /namespaces/{ns}/jobs ---

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: EntityId,
    /// Marquez job type. We always report `BATCH`; the event log doesn't
    /// distinguish stream/service jobs.
    #[serde(rename = "type")]
    pub job_type: String,
    pub name: String,
    pub simple_name: String,
    pub namespace: String,
    pub created_at: String,
    pub updated_at: String,
    pub inputs: Vec<EntityId>,
    pub outputs: Vec<EntityId>,
    pub location: Option<String>,
    pub description: Option<String>,
    pub latest_run: Option<LatestRun>,
    /// The job's reconstructed runs, newest first. We fold the stored
    /// `event_type` + `run_id` columns into per-run state (see
    /// [`RunAgg`](crate::read::queries)) so failed/running jobs render with the
    /// real state rather than a fabricated `COMPLETED`. Always non-empty for a
    /// known job — the dashboard's `JobRunItem` reduces over `latestRuns` with
    /// no initial value and crashes on an empty array, so jobs with no run-typed
    /// events still carry one neutral entry.
    pub latest_runs: Vec<LatestRun>,
    pub tags: Vec<String>,
    pub parent_job_name: Option<String>,
    pub parent_job_uuid: Option<String>,
}

/// Minimal Marquez `Run` shape — only the fields the web UI dereferences
/// (`id`, `state`, `durationMs`, the timestamps). Reconstructed from the stored
/// `event_type` + `run_id` columns; see [`RunAgg`](crate::read::queries).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LatestRun {
    pub id: String,
    pub created_at: String,
    pub updated_at: String,
    /// `NEW` | `RUNNING` | `COMPLETED` | `FAILED` | `ABORTED`.
    pub state: String,
    pub nominal_start_time: Option<String>,
    pub nominal_end_time: Option<String>,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub duration_ms: u64,
}

impl LatestRun {
    /// A single neutral run for jobs whose events carry no `run_id` (pure `job`
    /// events). The dashboard's `latestRuns.reduce(...)` has no initial value
    /// and crashes on an empty array, so we always emit at least this. State is
    /// `COMPLETED` (not flagged as failed); `durationMs` 0 renders a minimal bar.
    pub fn neutral(job_id: &str, updated_at: &str) -> Self {
        Self {
            id: format!("norun:{job_id}"),
            created_at: updated_at.to_string(),
            updated_at: updated_at.to_string(),
            state: "COMPLETED".to_string(),
            nominal_start_time: None,
            nominal_end_time: None,
            started_at: None,
            ended_at: None,
            duration_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Jobs {
    pub jobs: Vec<Job>,
    pub total_count: usize,
}

/// `GET /api/v1/namespaces/{ns}/jobs/{job}/runs` envelope. The UI's runs
/// reducer reads `payload.runs` and `totalCount`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Runs {
    pub runs: Vec<LatestRun>,
    pub total_count: usize,
}

// --- /namespaces/{ns}/datasets ---

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: EntityId,
    /// Marquez dataset type; we always report `DB_TABLE`.
    #[serde(rename = "type")]
    pub dataset_type: String,
    pub name: String,
    pub physical_name: String,
    pub namespace: String,
    pub source_name: String,
    pub created_at: String,
    pub updated_at: String,
    pub description: Option<String>,
    pub fields: Vec<serde_json::Value>,
    pub facets: serde_json::Value,
    pub tags: Vec<String>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Datasets {
    pub datasets: Vec<Dataset>,
    pub total_count: usize,
}

// --- /search ---

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    pub name: String,
    pub namespace: String,
    pub node_id: String,
    /// `JOB` or `DATASET`.
    #[serde(rename = "type")]
    pub result_type: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Search {
    pub total_count: usize,
    pub results: Vec<SearchResult>,
}

// --- /events/lineage ---

/// `GET /api/v1/events/lineage` envelope — the Events page. Each event is the
/// raw OpenLineage event JSON as ingested (`raw_json`); the UI dereferences
/// `eventType`, `eventTime`, `run`, `job`, `inputs`, `outputs`, `producer`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageEvents {
    pub events: Vec<serde_json::Value>,
    pub total_count: usize,
}

// --- /namespaces/{ns}/datasets/{ds}/versions ---

/// One Marquez `DatasetVersion`. We reconstruct one version per distinct
/// schema-bearing snapshot of the dataset; `version` is a deterministic id
/// derived from the namespace/name/fields so the same schema yields a stable id.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetVersion {
    pub id: DatasetVersionId,
    #[serde(rename = "type")]
    pub dataset_type: String,
    pub name: String,
    pub physical_name: String,
    pub created_at: String,
    pub version: String,
    pub namespace: String,
    pub source_name: String,
    pub fields: Vec<serde_json::Value>,
    pub tags: Vec<String>,
    pub last_modified_at: Option<String>,
    pub description: Option<String>,
    pub facets: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetVersionId {
    pub namespace: String,
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetVersions {
    pub versions: Vec<DatasetVersion>,
    pub total_count: usize,
}

// --- /jobs/runs/{id}/facets ---

/// `GET /api/v1/jobs/runs/{id}/facets` — the run-detail facets tab. `facets` is
/// a map keyed by facet name; we surface the run facets carried on the run's
/// raw OpenLineage events.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunFacets {
    pub run_id: String,
    pub facets: serde_json::Value,
}

// --- /column-lineage ---

/// `GET /api/v1/column-lineage` envelope: `DATASET_FIELD` nodes (one per
/// output field of the addressed dataset plus one per referenced input field)
/// with edges mirroring the stored facet's `inputFields`. Datasets without
/// column lineage yield an empty graph (200, not 404) so the UI's column view
/// renders empty.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnLineageGraph {
    pub graph: Vec<LineageNode>,
}

/// Build the Marquez `nodeId` for a dataset field.
pub fn dataset_field_node_id(namespace: &str, dataset: &str, field: &str) -> String {
    format!("datasetField:{namespace}:{dataset}:{field}")
}

/// Parse the `nodeId` forms the column-lineage endpoint accepts:
/// `dataset:<ns>:<name>` (all fields) or `datasetField:<ns>:<name>:<field>`
/// (one field). Returns `(namespace, dataset, Some(field))` for the latter.
pub fn parse_column_lineage_node_id(node_id: &str) -> Option<(String, String, Option<String>)> {
    if let Some((NodeKind::Dataset, namespace, name)) = parse_node_id(node_id) {
        return Some((namespace, name, None));
    }
    let rest = node_id.strip_prefix("datasetField:")?;
    let (namespace, tail) = split_namespace_name(rest)?;
    // The dataset name may itself contain `:`; the field is the last segment.
    let (dataset, field) = tail.rsplit_once(':')?;
    Some((
        namespace.to_string(),
        dataset.to_string(),
        Some(field.to_string()),
    ))
}

// --- /lineage ---

/// A directed edge between two nodes, addressed by their `nodeId` strings.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Hash)]
pub struct LineageEdge {
    pub origin: String,
    pub destination: String,
}

/// One node in the lineage graph. `data` carries the full [`Job`] or [`Dataset`]
/// payload the UI renders in the side panel. Note the `camelCase` rename: the
/// UI's graph layout reads `node.inEdges` / `node.outEdges` and crashes
/// (`.map()` of undefined) if they arrive as snake_case.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LineageNode {
    /// The `nodeId`, e.g. `job:ns:name` or `dataset:ns:name`.
    pub id: String,
    /// `JOB` or `DATASET`.
    #[serde(rename = "type")]
    pub node_type: String,
    pub data: serde_json::Value,
    pub in_edges: Vec<LineageEdge>,
    pub out_edges: Vec<LineageEdge>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LineageGraph {
    pub graph: Vec<LineageNode>,
}

/// Build the Marquez `nodeId` for a job.
pub fn job_node_id(namespace: &str, name: &str) -> String {
    format!("job:{namespace}:{name}")
}

/// Build the Marquez `nodeId` for a dataset.
pub fn dataset_node_id(namespace: &str, name: &str) -> String {
    format!("dataset:{namespace}:{name}")
}

/// The two node kinds a `nodeId` can address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Job,
    Dataset,
}

/// Parse a Marquez `nodeId` of the form `job:<namespace>:<name>` or
/// `dataset:<namespace>:<name>`.
///
/// The namespace itself is frequently a URI (`open-lineage` emits
/// `s3://bucket`-style dataset namespaces per the OpenLineage naming spec), so a
/// naive "split on the first two `:`" parse mangles `dataset:s3://bucket:wh/t1`
/// into namespace `s3`, name `//bucket:wh/t1`. We mirror Marquez's NodeId
/// parsing: when the text after the kind prefix begins with a URI scheme
/// (`[a-z][a-z0-9+.-]*://`), the namespace extends through the authority and the
/// namespace/name boundary is the next `:` *after* the authority; otherwise it's
/// the first `:`. The name may still contain further `:`.
pub fn parse_node_id(node_id: &str) -> Option<(NodeKind, String, String)> {
    let (kind, rest) = node_id.split_once(':')?;
    let kind = match kind {
        "job" => NodeKind::Job,
        "dataset" => NodeKind::Dataset,
        _ => return None,
    };
    let (namespace, name) = split_namespace_name(rest)?;
    Some((kind, namespace.to_string(), name.to_string()))
}

/// Split the `<namespace>:<name>` tail of a nodeId, honoring URI-style
/// namespaces. Returns `None` when there is no namespace/name separator.
fn split_namespace_name(rest: &str) -> Option<(&str, &str)> {
    // Search for the namespace/name boundary `:` starting *after* any
    // `scheme://authority` prefix so URI authorities aren't split apart.
    let search_from = scheme_authority_end(rest).unwrap_or_default();
    let offset = rest[search_from..].find(':')?;
    let boundary = search_from + offset;
    Some((&rest[..boundary], &rest[boundary + 1..]))
}

/// If `s` begins with a URI scheme (`[a-z][a-z0-9+.-]*://`), return the byte
/// offset of the end of its `scheme://authority` prefix (i.e. the position of
/// the `/` or `:` that terminates the authority, or the string end). Returns
/// `None` when `s` does not start with a scheme.
fn scheme_authority_end(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    // scheme: leading letter, then letters/digits/`+`/`-`/`.`
    if bytes.is_empty() || !bytes[0].is_ascii_lowercase() {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_alphanumeric() || matches!(c, b'+' | b'-' | b'.') {
            i += 1;
        } else {
            break;
        }
    }
    // require `://` immediately after the scheme
    if !s[i..].starts_with("://") {
        return None;
    }
    // authority runs from after `://` up to the next `/` or `:` (or end).
    let auth_start = i + 3;
    let auth_len = s[auth_start..]
        .find(['/', ':'])
        .unwrap_or(s.len() - auth_start);
    Some(auth_start + auth_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_namespace() {
        let (kind, ns, name) = parse_node_id("dataset:ns:name").unwrap();
        assert_eq!(kind, NodeKind::Dataset);
        assert_eq!(ns, "ns");
        assert_eq!(name, "name");
    }

    #[test]
    fn parse_job_node_id() {
        let (kind, ns, name) = parse_node_id("job:my-ns:etl.daily").unwrap();
        assert_eq!(kind, NodeKind::Job);
        assert_eq!(ns, "my-ns");
        assert_eq!(name, "etl.daily");
    }

    #[test]
    fn parse_uri_namespace() {
        // The crux of C3: the s3:// authority must stay in the namespace.
        let (kind, ns, name) = parse_node_id("dataset:s3://bucket:warehouse/t1").unwrap();
        assert_eq!(kind, NodeKind::Dataset);
        assert_eq!(ns, "s3://bucket");
        assert_eq!(name, "warehouse/t1");
    }

    #[test]
    fn parse_uri_namespace_with_slash_boundary() {
        // No `:` after the authority — the name starts right after the authority,
        // which here means the boundary is the `:` between ns and the path-name.
        let (_, ns, name) = parse_node_id("dataset:s3://open-lakehouse:warehouse/db/t").unwrap();
        assert_eq!(ns, "s3://open-lakehouse");
        assert_eq!(name, "warehouse/db/t");
    }

    #[test]
    fn parse_uri_namespace_with_port_in_name() {
        // A name containing further `:` is preserved beyond the first boundary.
        let (_, ns, name) = parse_node_id("dataset:postgres://host:db.public.t:extra").unwrap();
        assert_eq!(ns, "postgres://host");
        assert_eq!(name, "db.public.t:extra");
    }

    #[test]
    fn round_trip_uri_dataset() {
        let id = dataset_node_id("s3://bucket", "warehouse/t1");
        let (kind, ns, name) = parse_node_id(&id).unwrap();
        assert_eq!(kind, NodeKind::Dataset);
        assert_eq!(ns, "s3://bucket");
        assert_eq!(name, "warehouse/t1");
    }

    #[test]
    fn parse_rejects_unknown_kind_and_missing_separator() {
        assert!(parse_node_id("widget:ns:name").is_none());
        assert!(parse_node_id("dataset:only-namespace").is_none());
        assert!(parse_node_id("nocolon").is_none());
    }
}
