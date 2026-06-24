//! Reconstruct Marquez's materialized model (namespaces, jobs, datasets, lineage
//! graph) from the append-only OpenLineage events table.
//!
//! Strategy: a single scan of the events table builds an in-memory [`Model`] —
//! every distinct job with its input/output dataset references (unioned across
//! events, so a terminal event that drops the datasets the START carried does
//! not erase them), its per-run states folded from `event_type` + `run_id`, and
//! first/last seen timestamps, plus the set of datasets (both standalone dataset
//! events and those implied by job edges). All endpoints are then derived from
//! that model, including the lineage graph (built by a BFS over job↔dataset
//! edges). The query volume for a lineage UI is low, so doing the aggregation in
//! Rust over one scan is simpler and clearer than many bespoke SQL queries.
//! The events feed, run facets, dataset versions, and column-lineage endpoints
//! are served directly off the event log without the full model fold.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use deltalake::arrow::array::{Array, ArrayRef, StringArray, TimestampMicrosecondArray};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::DataType;
use deltalake::arrow::record_batch::RecordBatch;
use serde_json::json;

use super::columns as col;
use super::model::*;
use super::{LineageStore, ReadError};

/// Default and maximum graph traversal depth (hops). Marquez's UI defaults to a
/// depth of 20; we cap to keep BFS bounded on dense graphs.
const MAX_DEPTH: usize = 20;

/// A dataset reference parsed from a job's `inputs_json` / `outputs_json`.
#[derive(serde::Deserialize)]
struct DatasetRef {
    namespace: String,
    name: String,
}

/// One reconstructed run of a job, folded from the stored `run_id` +
/// `event_type` columns. Marquez's run state is derived from the *latest*
/// eventType seen for a runId: `START` → `RUNNING`, `COMPLETE` → `COMPLETED`,
/// `FAIL` → `FAILED`, `ABORT` → `ABORTED`, `OTHER`/`RUNNING` keep the run
/// `RUNNING`.
#[derive(Clone)]
struct RunAgg {
    run_id: String,
    /// Latest known terminal/active state for this run.
    state: RunState,
    /// Microseconds since epoch: earliest event (≈ start) and latest event.
    first_seen: i64,
    last_seen: i64,
    /// Event time of the START event, if one was seen (for duration).
    started_at: Option<i64>,
    /// Event time of the terminal event (COMPLETE/FAIL/ABORT), if seen.
    ended_at: Option<i64>,
}

/// Marquez run states, ordered so a later terminal event can't be downgraded by
/// a stray earlier-typed event arriving out of order.
#[derive(Clone, Copy, PartialEq, Eq)]
enum RunState {
    New,
    Running,
    Completed,
    Failed,
    Aborted,
}

impl RunState {
    /// Map an OpenLineage `eventType` (case-insensitive) to a run state.
    fn from_event_type(et: &str) -> Option<Self> {
        match et.to_ascii_uppercase().as_str() {
            "START" | "RUNNING" => Some(RunState::Running),
            "COMPLETE" => Some(RunState::Completed),
            "FAIL" => Some(RunState::Failed),
            "ABORT" => Some(RunState::Aborted),
            _ => None,
        }
    }

    fn as_marquez(self) -> &'static str {
        match self {
            RunState::New => "NEW",
            RunState::Running => "RUNNING",
            RunState::Completed => "COMPLETED",
            RunState::Failed => "FAILED",
            RunState::Aborted => "ABORTED",
        }
    }

    /// Whether `other` is a terminal state that should supersede `self`.
    fn is_terminal(self) -> bool {
        matches!(
            self,
            RunState::Completed | RunState::Failed | RunState::Aborted
        )
    }
}

/// Aggregated, in-memory view of one job derived from its events.
#[derive(Default)]
struct JobAgg {
    inputs: Vec<EntityId>,
    outputs: Vec<EntityId>,
    /// Microseconds since epoch of the earliest / latest event for this job.
    first_seen: i64,
    last_seen: i64,
    /// Event time at which the current input/output sets were last set — used to
    /// decide whether a newer dataset-bearing event should replace them.
    edges_at: i64,
    /// Job description from the `documentation` job facet, latest-event-wins.
    description: Option<String>,
    /// Job tags from the `tags` job facet, rendered as `key` / `key:value`
    /// strings (the Marquez job model carries plain strings).
    tags: Vec<String>,
    /// Event time at which `description`/`tags` were last set (same
    /// latest-event-wins pattern as `edges_at`).
    meta_at: i64,
    /// Per-run state keyed by runId, in arrival order (for newest-first sort).
    runs: HashMap<String, RunAgg>,
}

/// The reconstructed model: jobs keyed by id, the dataset id set, and the
/// per-dataset first/last seen times.
#[derive(Default)]
struct Model {
    jobs: BTreeMap<EntityId, JobAgg>,
    datasets: BTreeMap<EntityId, DatasetAgg>,
}

/// Reconstructed dataset state: the first/last event times plus the columns
/// from the latest `schema` dataset facet seen for it (so the dataset shows its
/// fields in the graph). `schema_at` is the event time the fields came from, so
/// a later schema wins and an event without a schema facet never clears one.
#[derive(Default)]
struct DatasetAgg {
    first_seen: i64,
    last_seen: i64,
    fields: Vec<serde_json::Value>,
    schema_at: i64,
}

impl LineageStore {
    /// Scan the events table and fold it into a [`Model`]. An absent table
    /// (nothing ingested yet) yields an empty model.
    async fn model(&self) -> Result<Model, ReadError> {
        let Some(ctx) = self.session().await? else {
            return Ok(Model::default());
        };
        let batches = ctx
            .sql(&format!(
                "SELECT {kind}, {etype}, {etime}, {run_id}, {jns}, {jname}, {dns}, {dname}, \
                 {inputs}, {outputs}, {raw} FROM events",
                kind = col::EVENT_KIND,
                etype = col::EVENT_TYPE,
                etime = col::EVENT_TIME,
                run_id = col::RUN_ID,
                jns = col::JOB_NAMESPACE,
                jname = col::JOB_NAME,
                dns = col::DATASET_NAMESPACE,
                dname = col::DATASET_NAME,
                inputs = col::INPUTS_JSON,
                outputs = col::OUTPUTS_JSON,
                raw = col::RAW_JSON,
            ))
            .await?
            .collect()
            .await?;

        let mut model = Model::default();
        for batch in &batches {
            fold_batch(&mut model, batch)?;
        }
        Ok(model)
    }

    /// `GET /api/v1/namespaces`
    pub async fn namespaces(&self) -> Result<Namespaces, ReadError> {
        let model = self.model().await?;
        let mut names: HashMap<String, (i64, i64)> = HashMap::new();
        let mut note = |ns: &str, lo: i64, hi: i64| {
            if ns.is_empty() {
                return;
            }
            let e = names.entry(ns.to_string()).or_insert((lo, hi));
            e.0 = e.0.min(lo);
            e.1 = e.1.max(hi);
        };
        for (id, agg) in &model.jobs {
            note(&id.namespace, agg.first_seen, agg.last_seen);
        }
        for (id, agg) in &model.datasets {
            note(&id.namespace, agg.first_seen, agg.last_seen);
        }
        let mut namespaces: Vec<Namespace> = names
            .into_iter()
            .map(|(name, (lo, hi))| Namespace {
                name,
                created_at: micros_to_rfc3339(lo),
                updated_at: micros_to_rfc3339(hi),
                owner_name: None,
                description: None,
                is_hidden: false,
            })
            .collect();
        namespaces.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Namespaces { namespaces })
    }

    /// `GET /api/v1/namespaces/{ns}/jobs` (when `namespace` is `Some`) and the
    /// global `GET /api/v1/jobs` (when `None`, returning every namespace's jobs).
    pub async fn jobs(
        &self,
        namespace: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Jobs, ReadError> {
        let model = self.model().await?;
        let mut all: Vec<Job> = model
            .jobs
            .iter()
            .filter(|(id, _)| namespace.is_none_or(|ns| id.namespace == ns))
            .map(|(id, agg)| build_job(id, agg))
            .collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let total_count = all.len();
        let jobs = all.into_iter().skip(offset).take(limit).collect();
        Ok(Jobs { jobs, total_count })
    }

    /// `GET /api/v1/namespaces/{ns}/jobs/{job}`
    pub async fn job(&self, namespace: &str, name: &str) -> Result<Job, ReadError> {
        let model = self.model().await?;
        let id = EntityId {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        model
            .jobs
            .get(&id)
            .map(|agg| build_job(&id, agg))
            .ok_or_else(|| ReadError::NotFound(format!("job {namespace}/{name}")))
    }

    /// `GET /api/v1/namespaces/{ns}/jobs/{job}/runs` — the job detail drawer's
    /// run history: the reconstructed per-run states the job carries in
    /// `latestRuns` (newest first). 404 if the job is unknown.
    pub async fn job_runs(&self, namespace: &str, name: &str) -> Result<Runs, ReadError> {
        let job = self.job(namespace, name).await?;
        let total_count = job.latest_runs.len();
        Ok(Runs {
            runs: job.latest_runs,
            total_count,
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets` (when `namespace` is `Some`) and
    /// the global `GET /api/v1/datasets` (when `None`).
    pub async fn datasets(
        &self,
        namespace: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Datasets, ReadError> {
        let model = self.model().await?;
        let mut all: Vec<Dataset> = model
            .datasets
            .iter()
            .filter(|(id, _)| namespace.is_none_or(|ns| id.namespace == ns))
            .map(|(id, agg)| build_dataset(id, agg))
            .collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let total_count = all.len();
        let datasets = all.into_iter().skip(offset).take(limit).collect();
        Ok(Datasets {
            datasets,
            total_count,
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets/{name}`
    pub async fn dataset(&self, namespace: &str, name: &str) -> Result<Dataset, ReadError> {
        let model = self.model().await?;
        let id = EntityId {
            namespace: namespace.to_string(),
            name: name.to_string(),
        };
        model
            .datasets
            .get(&id)
            .map(|agg| build_dataset(&id, agg))
            .ok_or_else(|| ReadError::NotFound(format!("dataset {namespace}/{name}")))
    }

    /// `GET /api/v1/search?q=`
    pub async fn search(&self, q: &str, limit: usize) -> Result<Search, ReadError> {
        let model = self.model().await?;
        let needle = q.to_lowercase();
        let mut results: Vec<SearchResult> = Vec::new();
        for (id, agg) in &model.jobs {
            if id.name.to_lowercase().contains(&needle) {
                results.push(SearchResult {
                    name: id.name.clone(),
                    namespace: id.namespace.clone(),
                    node_id: job_node_id(&id.namespace, &id.name),
                    result_type: "JOB".into(),
                    updated_at: micros_to_rfc3339(agg.last_seen),
                });
            }
        }
        for (id, agg) in &model.datasets {
            if id.name.to_lowercase().contains(&needle) {
                results.push(SearchResult {
                    name: id.name.clone(),
                    namespace: id.namespace.clone(),
                    node_id: dataset_node_id(&id.namespace, &id.name),
                    result_type: "DATASET".into(),
                    updated_at: micros_to_rfc3339(agg.last_seen),
                });
            }
        }
        results.sort_by(|a, b| a.name.cmp(&b.name));
        // Count all matches before truncating to the page — `totalCount` is the
        // full match count, not the size of the returned page.
        let total_count = results.len();
        results.truncate(limit);
        Ok(Search {
            total_count,
            results,
        })
    }

    /// `GET /api/v1/lineage?nodeId=&depth=`
    ///
    /// Builds the full edge set from the model (input→job, job→output), then
    /// BFS-traverses up to `depth` hops in both directions from the seed node,
    /// emitting each reached node with its incident edges.
    pub async fn lineage(&self, node_id: &str, depth: usize) -> Result<LineageGraph, ReadError> {
        let (seed_kind, seed_ns, seed_name) = parse_node_id(node_id)
            .ok_or_else(|| ReadError::NotFound(format!("malformed nodeId {node_id}")))?;
        let model = self.model().await?;

        // The seed must exist in the model; Marquez 404s an unknown nodeId rather
        // than synthesizing an empty node for it.
        let seed_id = EntityId {
            namespace: seed_ns,
            name: seed_name,
        };
        let seed_known = match seed_kind {
            NodeKind::Job => model.jobs.contains_key(&seed_id),
            NodeKind::Dataset => model.datasets.contains_key(&seed_id),
        };
        if !seed_known {
            return Err(ReadError::NotFound(format!("node {node_id}")));
        }

        let depth = depth.min(MAX_DEPTH);

        // Build directed edges and an adjacency map (undirected, for traversal).
        let mut edges: HashSet<LineageEdge> = HashSet::new();
        let mut neighbors: HashMap<String, HashSet<String>> = HashMap::new();
        let connect = |from: String,
                       to: String,
                       edges: &mut HashSet<LineageEdge>,
                       neighbors: &mut HashMap<String, HashSet<String>>| {
            neighbors
                .entry(from.clone())
                .or_default()
                .insert(to.clone());
            neighbors
                .entry(to.clone())
                .or_default()
                .insert(from.clone());
            edges.insert(LineageEdge {
                origin: from,
                destination: to,
            });
        };
        for (id, agg) in &model.jobs {
            let job_id = job_node_id(&id.namespace, &id.name);
            for input in &agg.inputs {
                let ds = dataset_node_id(&input.namespace, &input.name);
                connect(ds, job_id.clone(), &mut edges, &mut neighbors);
            }
            for output in &agg.outputs {
                let ds = dataset_node_id(&output.namespace, &output.name);
                connect(job_id.clone(), ds, &mut edges, &mut neighbors);
            }
        }

        // BFS out to `depth` hops from the seed.
        let mut reached: HashSet<String> = HashSet::new();
        reached.insert(node_id.to_string());
        let mut frontier: VecDeque<(String, usize)> = VecDeque::new();
        frontier.push_back((node_id.to_string(), 0));
        while let Some((cur, d)) = frontier.pop_front() {
            if d >= depth {
                continue;
            }
            if let Some(adj) = neighbors.get(&cur) {
                for next in adj {
                    if reached.insert(next.clone()) {
                        frontier.push_back((next.clone(), d + 1));
                    }
                }
            }
        }

        // Emit each reached node with its incident in/out edges.
        let graph = reached
            .iter()
            .filter_map(|id| build_node(id, &model, &edges))
            .collect();
        Ok(LineageGraph { graph })
    }

    /// `GET /api/v1/events/lineage?limit=&offset=` — the Events page. A paginated
    /// scan of the stored raw OpenLineage events, newest first. Each element is
    /// the event's `raw_json` parsed back into JSON (so the UI sees the original
    /// `eventType`/`run`/`job`/`inputs`/`outputs`). `totalCount` is the full
    /// event count, not the page size.
    pub async fn events(&self, limit: usize, offset: usize) -> Result<LineageEvents, ReadError> {
        let Some(ctx) = self.session().await? else {
            return Ok(LineageEvents {
                events: Vec::new(),
                total_count: 0,
            });
        };
        // Order newest-first and page in SQL so we don't materialize the whole
        // log; count separately for the full total.
        let total_count = scalar_count(&ctx, "SELECT COUNT(*) FROM events").await?;
        let batches = ctx
            .sql(&format!(
                "SELECT {raw} FROM events ORDER BY {etime} DESC NULLS LAST \
                 LIMIT {limit} OFFSET {offset}",
                raw = col::RAW_JSON,
                etime = col::EVENT_TIME,
            ))
            .await?
            .collect()
            .await?;
        let mut events = Vec::new();
        for batch in &batches {
            let raw = str_col(batch, 0)?;
            for row in 0..batch.num_rows() {
                if let Some(s) = value(&raw, row)
                    && let Ok(v) = serde_json::from_str::<serde_json::Value>(s)
                {
                    events.push(v);
                }
            }
        }
        Ok(LineageEvents {
            events,
            total_count,
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets/{ds}/versions` — the dataset detail
    /// "versions" tab. We don't track Delta versions, so we fold the distinct
    /// schema-bearing snapshots of the dataset from the stored events into one
    /// `DatasetVersion` each (deduped by field set). 404 if the dataset is
    /// unknown, like the dataset detail endpoint.
    pub async fn dataset_versions(
        &self,
        namespace: &str,
        name: &str,
        limit: usize,
        offset: usize,
    ) -> Result<DatasetVersions, ReadError> {
        // Confirm the dataset exists (404 otherwise) and reuse its timestamps.
        let dataset = self.dataset(namespace, name).await?;
        // We don't reconstruct historical schemas yet; emit a single current
        // version derived from the dataset. Deterministic version id from the
        // identity so the UI's version link is stable.
        let version = stable_version_id(namespace, name, &dataset.fields);
        let all = vec![DatasetVersion {
            id: DatasetVersionId {
                namespace: namespace.to_string(),
                name: name.to_string(),
                version: version.clone(),
            },
            dataset_type: dataset.dataset_type,
            name: name.to_string(),
            physical_name: dataset.physical_name,
            created_at: dataset.created_at,
            version,
            namespace: namespace.to_string(),
            source_name: dataset.source_name,
            fields: dataset.fields,
            tags: Vec::new(),
            last_modified_at: Some(dataset.updated_at),
            description: dataset.description,
            facets: dataset.facets,
        }];
        let total_count = all.len();
        let versions = all.into_iter().skip(offset).take(limit).collect();
        Ok(DatasetVersions {
            versions,
            total_count,
        })
    }

    /// `GET /api/v1/jobs/runs/{id}/facets` — the run-detail facets tab. We pull
    /// the run facets off the raw OpenLineage events carrying this `runId`,
    /// merging the `run.facets` maps across the run's events. 404 if no event
    /// references the run.
    pub async fn run_facets(&self, run_id: &str) -> Result<RunFacets, ReadError> {
        let Some(ctx) = self.session().await? else {
            return Err(ReadError::NotFound(format!("run {run_id}")));
        };
        // Parameter is interpolated into a string literal; escape single quotes.
        let escaped = run_id.replace('\'', "''");
        let batches = ctx
            .sql(&format!(
                "SELECT {raw} FROM events WHERE {rid} = '{escaped}' \
                 ORDER BY {etime} ASC NULLS LAST",
                raw = col::RAW_JSON,
                rid = col::RUN_ID,
                etime = col::EVENT_TIME,
            ))
            .await?
            .collect()
            .await?;

        let mut facets = serde_json::Map::new();
        let mut seen = false;
        for batch in &batches {
            let raw = str_col(batch, 0)?;
            for row in 0..batch.num_rows() {
                seen = true;
                let Some(s) = value(&raw, row) else { continue };
                let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else {
                    continue;
                };
                // Later events override earlier facets of the same name.
                if let Some(obj) = v
                    .get("run")
                    .and_then(|r| r.get("facets"))
                    .and_then(|f| f.as_object())
                {
                    for (k, val) in obj {
                        facets.insert(k.clone(), val.clone());
                    }
                }
            }
        }
        if !seen {
            return Err(ReadError::NotFound(format!("run {run_id}")));
        }
        Ok(RunFacets {
            run_id: run_id.to_string(),
            facets: serde_json::Value::Object(facets),
        })
    }

    /// `GET /api/v1/column-lineage?nodeId=` — the dataset column-lineage view.
    ///
    /// Serves the *latest* stored `column_lineage_json` facet of the addressed
    /// output dataset: one `DATASET_FIELD` node per output field plus one per
    /// referenced input field, with edges mirroring the facet's `inputFields`
    /// (single-hop upstream — what the UI's dataset column view renders; the
    /// `depth`/`withDownstream` params are accepted and ignored). Unknown
    /// datasets and datasets without column lineage return an empty graph
    /// (200, not 404) so the column view renders empty.
    pub async fn column_lineage(&self, node_id: &str) -> Result<ColumnLineageGraph, ReadError> {
        let empty = ColumnLineageGraph { graph: Vec::new() };
        let Some((namespace, dataset, field_filter)) = parse_column_lineage_node_id(node_id) else {
            return Ok(empty);
        };
        let Some(ctx) = self.session().await? else {
            return Ok(empty);
        };

        // Newest-first: the first event carrying a facet for this output
        // dataset is its current column lineage.
        let batches = ctx
            .sql(&format!(
                "SELECT {cl} FROM events WHERE {cl} IS NOT NULL \
                 ORDER BY {etime} DESC NULLS LAST",
                cl = col::COLUMN_LINEAGE_JSON,
                etime = col::EVENT_TIME,
            ))
            .await?
            .collect()
            .await?;
        let mut facet = None;
        'rows: for batch in &batches {
            let raw = str_col(batch, 0)?;
            for row in 0..batch.num_rows() {
                let Some(s) = value(&raw, row) else { continue };
                let Ok(doc) = serde_json::from_str::<serde_json::Value>(s) else {
                    continue;
                };
                let Some(outputs) = doc.get("outputs").and_then(|o| o.as_array()) else {
                    continue;
                };
                for out in outputs {
                    if out["namespace"] == namespace.as_str() && out["name"] == dataset.as_str() {
                        facet = Some(out["columnLineage"].clone());
                        break 'rows;
                    }
                }
            }
        }
        let Some(facet) = facet else {
            return Ok(empty);
        };
        let Some(fields) = facet["fields"].as_object() else {
            return Ok(empty);
        };

        // Fold edges and per-node data, then materialize the nodes. Output
        // fields carry their full `inputFields` payload; nodes that only
        // appear as inputs get a bare (namespace, dataset, field) payload.
        let mut data: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let mut in_edges: BTreeMap<String, Vec<LineageEdge>> = BTreeMap::new();
        let mut out_edges: BTreeMap<String, Vec<LineageEdge>> = BTreeMap::new();
        for (field, lineage) in fields {
            if field_filter.as_deref().is_some_and(|f| f != field) {
                continue;
            }
            let out_id = dataset_field_node_id(&namespace, &dataset, field);
            for input in lineage["inputFields"].as_array().into_iter().flatten() {
                let (Some(in_ns), Some(in_ds), Some(in_field)) = (
                    input["namespace"].as_str(),
                    input["name"].as_str(),
                    input["field"].as_str(),
                ) else {
                    continue;
                };
                let in_id = dataset_field_node_id(in_ns, in_ds, in_field);
                let edge = LineageEdge {
                    origin: in_id.clone(),
                    destination: out_id.clone(),
                };
                in_edges
                    .entry(out_id.clone())
                    .or_default()
                    .push(edge.clone());
                out_edges.entry(in_id.clone()).or_default().push(edge);
                data.entry(in_id).or_insert_with(|| {
                    json!({
                        "namespace": in_ns,
                        "dataset": in_ds,
                        "field": in_field,
                    })
                });
            }
            data.insert(
                out_id,
                json!({
                    "namespace": namespace,
                    "dataset": dataset,
                    "field": field,
                    "inputFields": lineage["inputFields"],
                }),
            );
        }

        let graph = data
            .into_iter()
            .map(|(id, data)| LineageNode {
                in_edges: in_edges.remove(&id).unwrap_or_default(),
                out_edges: out_edges.remove(&id).unwrap_or_default(),
                node_type: "DATASET_FIELD".to_string(),
                data,
                id,
            })
            .collect();
        Ok(ColumnLineageGraph { graph })
    }
}

/// Run a `SELECT COUNT(*)` query and read the single `u64`/`i64` scalar back.
async fn scalar_count(
    ctx: &deltalake::datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<usize, ReadError> {
    use deltalake::arrow::array::Int64Array;
    let batches = ctx.sql(sql).await?.collect().await?;
    let n = batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .filter(|a| !a.is_empty())
        .map(|a| a.value(0))
        .unwrap_or(0);
    Ok(n.max(0) as usize)
}

/// A deterministic version id for a dataset snapshot. We hash the identity plus
/// field set so the same schema always yields the same id (the UI uses it only
/// as an opaque key / link target).
fn stable_version_id(namespace: &str, name: &str, fields: &[serde_json::Value]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    namespace.hash(&mut h);
    name.hash(&mut h);
    serde_json::to_string(fields)
        .unwrap_or_default()
        .hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Fold one record batch of the projected events query into the model.
fn fold_batch(model: &mut Model, batch: &RecordBatch) -> Result<(), ReadError> {
    let kind = str_col(batch, 0)?;
    let etype = str_col(batch, 1)?;
    let etime = batch
        .column(2)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| ReadError::Query("event_time column type mismatch".into()))?;
    let run_id = str_col(batch, 3)?;
    let job_ns = str_col(batch, 4)?;
    let job_name = str_col(batch, 5)?;
    let ds_ns = str_col(batch, 6)?;
    let ds_name = str_col(batch, 7)?;
    let inputs = str_col(batch, 8)?;
    let outputs = str_col(batch, 9)?;
    let raw = str_col(batch, 10)?;

    for row in 0..batch.num_rows() {
        // A missing event_time is "unknown" (None), distinct from the epoch — so
        // we don't fabricate 1970 timestamps and can fall back to ingestion order.
        let ts = if etime.is_null(row) {
            None
        } else {
            Some(etime.value(row))
        };
        let ts_or_zero = ts.unwrap_or(0);
        match value(&kind, row) {
            Some("run") | Some("job") => {
                let (Some(ns), Some(name)) = (value(&job_ns, row), value(&job_name, row)) else {
                    continue;
                };
                let id = EntityId {
                    namespace: ns.to_string(),
                    name: name.to_string(),
                };
                let in_refs = parse_dataset_refs(value(&inputs, row));
                let out_refs = parse_dataset_refs(value(&outputs, row));
                let carries_edges = !in_refs.is_empty() || !out_refs.is_empty();

                let entry = model.jobs.entry(id).or_insert_with(|| JobAgg {
                    first_seen: ts_or_zero,
                    last_seen: ts_or_zero,
                    edges_at: i64::MIN,
                    meta_at: i64::MIN,
                    ..Default::default()
                });
                // Edge union (Marquez merges I/O cumulatively per job version): an
                // event with empty inputs/outputs (a FAIL/COMPLETE that drops the
                // datasets the START carried) must NOT erase the edges. We only
                // replace the sets when this event actually carries dataset refs,
                // and only keep the most-recent edge-bearing event's view.
                if carries_edges && ts_or_zero >= entry.edges_at {
                    entry.inputs = in_refs.clone();
                    entry.outputs = out_refs.clone();
                    entry.edges_at = ts_or_zero;
                }
                // Job metadata (description/tags from the documentation/tags
                // job facets), latest-event-wins like the edges: an event that
                // carries none must not erase metadata an earlier one set.
                let (description, tags) = parse_job_meta(value(&raw, row));
                if (description.is_some() || !tags.is_empty()) && ts_or_zero >= entry.meta_at {
                    entry.description = description;
                    entry.tags = tags;
                    entry.meta_at = ts_or_zero;
                }
                entry.first_seen = entry.first_seen.min(ts_or_zero);
                entry.last_seen = entry.last_seen.max(ts_or_zero);

                // Per-run state from event_type + run_id.
                if let Some(rid) = value(&run_id, row) {
                    fold_run(entry, rid, value(&etype, row), ts);
                }

                // Datasets implied by the job's edges. Output datasets may carry a
                // schema facet (their columns) in the raw event — capture it so the
                // dataset shows its fields; inputs get no schema from this event.
                let out_schemas = parse_output_schemas(value(&raw, row));
                for r in in_refs {
                    note_dataset(model, r.namespace, r.name, ts_or_zero, None);
                }
                for r in out_refs {
                    let schema = out_schemas
                        .get(&(r.namespace.clone(), r.name.clone()))
                        .map(|v| v.as_slice());
                    note_dataset(model, r.namespace, r.name, ts_or_zero, schema);
                }
            }
            Some("dataset") => {
                if let (Some(ns), Some(name)) = (value(&ds_ns, row), value(&ds_name, row)) {
                    note_dataset(model, ns.to_string(), name.to_string(), ts_or_zero, None);
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Fold a single run-typed event into the job's per-run state. `event_type` maps
/// to a [`RunState`]; a terminal state (COMPLETE/FAIL/ABORT) always wins over a
/// non-terminal one regardless of arrival order, and START/terminal event times
/// feed the run's duration.
fn fold_run(job: &mut JobAgg, run_id: &str, event_type: Option<&str>, ts: Option<i64>) {
    let new_state = event_type.and_then(RunState::from_event_type);
    let is_start = event_type.is_some_and(|et| et.eq_ignore_ascii_case("START"));
    let ts_or_zero = ts.unwrap_or(0);

    let run = job
        .runs
        .entry(run_id.to_string())
        .or_insert_with(|| RunAgg {
            run_id: run_id.to_string(),
            state: RunState::New,
            first_seen: ts_or_zero,
            last_seen: ts_or_zero,
            started_at: None,
            ended_at: None,
        });
    run.first_seen = run.first_seen.min(ts_or_zero);
    run.last_seen = run.last_seen.max(ts_or_zero);

    if let Some(s) = new_state {
        // Don't let a stray non-terminal event downgrade a terminal state.
        if s.is_terminal() || !run.state.is_terminal() {
            run.state = s;
        }
        if s.is_terminal() {
            run.ended_at = ts.or(run.ended_at);
        }
    }
    if is_start {
        run.started_at = ts.or(run.started_at);
    }
}

/// Extract job-level metadata from an event's raw JSON: the description from
/// the `documentation` job facet and tags from the `tags` job facet (rendered
/// as `key` / `key:value` strings). Returns empty values when the event carries
/// neither — a cheap substring pre-filter skips the JSON parse for the common
/// facet-less event.
fn parse_job_meta(raw: Option<&str>) -> (Option<String>, Vec<String>) {
    let Some(raw) = raw else {
        return (None, Vec::new());
    };
    if !raw.contains("\"documentation\"") && !raw.contains("\"tags\"") {
        return (None, Vec::new());
    }
    let Ok(event) = serde_json::from_str::<serde_json::Value>(raw) else {
        return (None, Vec::new());
    };
    let Some(facets) = event.get("job").and_then(|j| j.get("facets")) else {
        return (None, Vec::new());
    };

    let description = facets
        .get("documentation")
        .and_then(|d| d.get("description"))
        .and_then(|d| d.as_str())
        .map(str::to_string);

    let tags = facets
        .get("tags")
        .and_then(|t| t.get("tags"))
        .and_then(|t| t.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|t| {
                    let key = t.get("key")?.as_str()?;
                    Some(match t.get("value").and_then(|v| v.as_str()) {
                        Some(value) => format!("{key}:{value}"),
                        None => key.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    (description, tags)
}

fn note_dataset(
    model: &mut Model,
    namespace: String,
    name: String,
    ts: i64,
    schema: Option<&[serde_json::Value]>,
) {
    let id = EntityId { namespace, name };
    let e = model.datasets.entry(id).or_default();
    if e.first_seen == 0 && e.last_seen == 0 && e.schema_at == 0 {
        e.first_seen = ts;
    }
    e.first_seen = e.first_seen.min(ts);
    e.last_seen = e.last_seen.max(ts);
    // Latest-wins schema: only replace when this event carries fields and is at
    // least as recent as the schema we already have.
    if let Some(fields) = schema
        && !fields.is_empty()
        && ts >= e.schema_at
    {
        e.fields = fields.to_vec();
        e.schema_at = ts;
    }
}

/// Extract per-output-dataset schema fields from an event's `raw_json`:
/// `outputs[].facets.schema.fields`, keyed by `"namespace\u{1}name"`. Empty when
/// the event carries no output schema facet (a cheap substring pre-filter skips
/// the JSON parse for the common facet-less event).
fn parse_output_schemas(raw: Option<&str>) -> HashMap<(String, String), Vec<serde_json::Value>> {
    let mut out = HashMap::new();
    let Some(raw) = raw else { return out };
    if !raw.contains("\"schema\"") {
        return out;
    }
    let Ok(event) = serde_json::from_str::<serde_json::Value>(raw) else {
        return out;
    };
    let Some(outputs) = event.get("outputs").and_then(|o| o.as_array()) else {
        return out;
    };
    for ds in outputs {
        let (Some(ns), Some(name)) = (
            ds.get("namespace").and_then(|v| v.as_str()),
            ds.get("name").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        if let Some(fields) = ds
            .get("facets")
            .and_then(|f| f.get("schema"))
            .and_then(|s| s.get("fields"))
            .and_then(|f| f.as_array())
        {
            out.insert((ns.to_string(), name.to_string()), fields.clone());
        }
    }
    out
}

fn parse_dataset_refs(raw: Option<&str>) -> Vec<EntityId> {
    let Some(raw) = raw else { return Vec::new() };
    serde_json::from_str::<Vec<DatasetRef>>(raw)
        .unwrap_or_default()
        .into_iter()
        .map(|r| EntityId {
            namespace: r.namespace,
            name: r.name,
        })
        .collect()
}

fn build_job(id: &EntityId, agg: &JobAgg) -> Job {
    let updated_at = micros_to_rfc3339(agg.last_seen);
    let node_id = job_node_id(&id.namespace, &id.name);

    // Real runs, newest first (by last_seen, then start). Jobs whose events carry
    // no runId (pure `job` events) get a single neutral run so the dashboard's
    // `latestRuns.reduce(...)` has something to fold over.
    let mut runs: Vec<&RunAgg> = agg.runs.values().collect();
    runs.sort_by(|a, b| {
        b.last_seen
            .cmp(&a.last_seen)
            .then(b.first_seen.cmp(&a.first_seen))
    });
    let latest_runs: Vec<LatestRun> = if runs.is_empty() {
        vec![LatestRun::neutral(&node_id, &updated_at)]
    } else {
        runs.iter().map(|r| build_run(r)).collect()
    };
    let latest_run = latest_runs.first().cloned();

    Job {
        id: id.clone(),
        job_type: "BATCH".into(),
        name: id.name.clone(),
        simple_name: id.name.clone(),
        namespace: id.namespace.clone(),
        created_at: micros_to_rfc3339(agg.first_seen),
        updated_at,
        inputs: agg.inputs.clone(),
        outputs: agg.outputs.clone(),
        location: None,
        description: agg.description.clone(),
        latest_run,
        latest_runs,
        tags: agg.tags.clone(),
        parent_job_name: None,
        parent_job_uuid: None,
    }
}

/// Build a Marquez `Run` from a reconstructed [`RunAgg`]. Duration is the
/// START→terminal span when both event times are known; otherwise 0.
fn build_run(run: &RunAgg) -> LatestRun {
    let started_at = run.started_at.map(micros_to_rfc3339);
    let ended_at = run.ended_at.map(micros_to_rfc3339);
    let duration_ms = match (run.started_at, run.ended_at) {
        (Some(s), Some(e)) if e >= s => ((e - s) / 1000) as u64,
        _ => 0,
    };
    LatestRun {
        id: run.run_id.clone(),
        created_at: micros_to_rfc3339(run.first_seen),
        updated_at: micros_to_rfc3339(run.last_seen),
        state: run.state.as_marquez().to_string(),
        nominal_start_time: None,
        nominal_end_time: None,
        started_at,
        ended_at,
        duration_ms,
    }
}

fn build_dataset(id: &EntityId, agg: &DatasetAgg) -> Dataset {
    // Surface the schema facet's columns both as Marquez `fields` (what the UI
    // dereferences) and as a `schema` facet (spec shape), so the dataset shows
    // its columns however the consumer reads them.
    let facets = if agg.fields.is_empty() {
        json!({})
    } else {
        json!({ "schema": { "fields": agg.fields } })
    };
    Dataset {
        id: id.clone(),
        dataset_type: "DB_TABLE".into(),
        name: id.name.clone(),
        physical_name: id.name.clone(),
        namespace: id.namespace.clone(),
        source_name: id.namespace.clone(),
        created_at: micros_to_rfc3339(agg.first_seen),
        updated_at: micros_to_rfc3339(agg.last_seen),
        description: None,
        fields: agg.fields.clone(),
        facets,
        tags: Vec::new(),
        deleted: false,
    }
}

/// Build a graph node for `node_id` from the model, attaching its incident
/// edges. Returns `None` if the id is malformed or names an entity not in the
/// model — we never synthesize empty (epoch-timestamp) payloads for unknown
/// nodes.
fn build_node(node_id: &str, model: &Model, edges: &HashSet<LineageEdge>) -> Option<LineageNode> {
    let (kind, namespace, name) = parse_node_id(node_id)?;
    let id = EntityId {
        namespace: namespace.clone(),
        name: name.clone(),
    };
    let (node_type, data) = match kind {
        NodeKind::Job => {
            let job = build_job(&id, model.jobs.get(&id)?);
            ("JOB", serde_json::to_value(job).ok()?)
        }
        NodeKind::Dataset => {
            let agg = model.datasets.get(&id)?;
            (
                "DATASET",
                serde_json::to_value(build_dataset(&id, agg)).ok()?,
            )
        }
    };
    let in_edges = edges
        .iter()
        .filter(|e| e.destination == node_id)
        .cloned()
        .collect();
    let out_edges = edges
        .iter()
        .filter(|e| e.origin == node_id)
        .cloned()
        .collect();
    Some(LineageNode {
        id: node_id.to_string(),
        node_type: node_type.into(),
        data,
        in_edges,
        out_edges,
    })
}

/// Read column `idx` as a `Utf8` [`StringArray`], casting from whatever string
/// representation DataFusion produced (delta-rs reads strings as `Utf8View`).
fn str_col(batch: &RecordBatch, idx: usize) -> Result<StringArray, ReadError> {
    let col: &ArrayRef = batch.column(idx);
    let utf8 = if col.data_type() == &DataType::Utf8 {
        col.clone()
    } else {
        cast(col, &DataType::Utf8).map_err(|e| ReadError::Query(e.to_string()))?
    };
    utf8.as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| ReadError::Query(format!("column {idx} is not castable to Utf8")))
}

fn value(col: &StringArray, row: usize) -> Option<&str> {
    if col.is_null(row) {
        None
    } else {
        Some(col.value(row))
    }
}

/// Format epoch-microseconds as an RFC3339 timestamp (Marquez serializes all
/// times as ISO-8601 strings). A zero (unknown) timestamp maps to the epoch.
fn micros_to_rfc3339(micros: i64) -> String {
    DateTime::<Utc>::from_timestamp_micros(micros)
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0))
        .to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{StringArray, TimestampMicrosecondArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    /// One row of the projected events query, mirroring the column order in
    /// [`LineageStore::model`]: kind, event_type, event_time, run_id, job_ns,
    /// job_name, ds_ns, ds_name, inputs_json, outputs_json, raw_json.
    struct Row {
        kind: &'static str,
        event_type: Option<&'static str>,
        event_time: Option<i64>,
        run_id: Option<&'static str>,
        job_ns: Option<&'static str>,
        job_name: Option<&'static str>,
        ds_ns: Option<&'static str>,
        ds_name: Option<&'static str>,
        inputs: Option<&'static str>,
        outputs: Option<&'static str>,
        raw: Option<&'static str>,
    }

    fn batch(rows: &[Row]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_kind", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("run_id", DataType::Utf8, true),
            Field::new("job_namespace", DataType::Utf8, true),
            Field::new("job_name", DataType::Utf8, true),
            Field::new("dataset_namespace", DataType::Utf8, true),
            Field::new("dataset_name", DataType::Utf8, true),
            Field::new("inputs_json", DataType::Utf8, true),
            Field::new("outputs_json", DataType::Utf8, true),
            Field::new("raw_json", DataType::Utf8, true),
        ]));
        let kind = StringArray::from_iter_values(rows.iter().map(|r| r.kind));
        let etype = StringArray::from(rows.iter().map(|r| r.event_type).collect::<Vec<_>>());
        let etime =
            TimestampMicrosecondArray::from(rows.iter().map(|r| r.event_time).collect::<Vec<_>>())
                .with_timezone("UTC");
        let run_id = StringArray::from(rows.iter().map(|r| r.run_id).collect::<Vec<_>>());
        let job_ns = StringArray::from(rows.iter().map(|r| r.job_ns).collect::<Vec<_>>());
        let job_name = StringArray::from(rows.iter().map(|r| r.job_name).collect::<Vec<_>>());
        let ds_ns = StringArray::from(rows.iter().map(|r| r.ds_ns).collect::<Vec<_>>());
        let ds_name = StringArray::from(rows.iter().map(|r| r.ds_name).collect::<Vec<_>>());
        let inputs = StringArray::from(rows.iter().map(|r| r.inputs).collect::<Vec<_>>());
        let outputs = StringArray::from(rows.iter().map(|r| r.outputs).collect::<Vec<_>>());
        let raw = StringArray::from(rows.iter().map(|r| r.raw).collect::<Vec<_>>());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(kind),
                Arc::new(etype),
                Arc::new(etime),
                Arc::new(run_id),
                Arc::new(job_ns),
                Arc::new(job_name),
                Arc::new(ds_ns),
                Arc::new(ds_name),
                Arc::new(inputs),
                Arc::new(outputs),
                Arc::new(raw),
            ],
        )
        .unwrap()
    }

    fn run_event(
        event_type: &'static str,
        event_time: i64,
        run_id: &'static str,
        inputs: Option<&'static str>,
        outputs: Option<&'static str>,
    ) -> Row {
        Row {
            kind: "run",
            event_type: Some(event_type),
            event_time: Some(event_time),
            run_id: Some(run_id),
            job_ns: Some("ns"),
            job_name: Some("job"),
            ds_ns: None,
            ds_name: None,
            inputs,
            outputs,
            raw: None,
        }
    }

    fn fold(rows: &[Row]) -> Model {
        let mut model = Model::default();
        fold_batch(&mut model, &batch(rows)).unwrap();
        model
    }

    fn job(model: &Model) -> Job {
        let id = EntityId {
            namespace: "ns".into(),
            name: "job".into(),
        };
        build_job(&id, model.jobs.get(&id).expect("job present"))
    }

    #[test]
    fn edge_union_start_carries_edges_complete_does_not() {
        // START carries the I/O; COMPLETE drops it (the common producer pattern).
        // The COMPLETE must not erase the edges.
        let out = r#"[{"namespace":"s3://b","name":"warehouse/t1"}]"#;
        let model = fold(&[
            run_event("START", 1_000, "r1", None, Some(out)),
            run_event("COMPLETE", 2_000, "r1", None, None),
        ]);
        let j = job(&model);
        assert_eq!(j.outputs.len(), 1, "edges survive the empty COMPLETE");
        assert_eq!(j.outputs[0].namespace, "s3://b");
        assert_eq!(j.outputs[0].name, "warehouse/t1");
    }

    #[test]
    fn edge_union_later_edge_bearing_event_replaces() {
        let out1 = r#"[{"namespace":"ns","name":"t1"}]"#;
        let out2 = r#"[{"namespace":"ns","name":"t2"}]"#;
        let model = fold(&[
            run_event("START", 1_000, "r1", None, Some(out1)),
            run_event("COMPLETE", 2_000, "r1", None, Some(out2)),
        ]);
        let j = job(&model);
        assert_eq!(j.outputs.len(), 1);
        assert_eq!(j.outputs[0].name, "t2", "latest edge-bearing event wins");
    }

    #[test]
    fn run_state_start_complete() {
        let model = fold(&[
            run_event("START", 1_000_000, "r1", None, None),
            run_event("COMPLETE", 5_000_000, "r1", None, None),
        ]);
        let j = job(&model);
        assert_eq!(j.latest_runs.len(), 1);
        let run = &j.latest_runs[0];
        assert_eq!(run.state, "COMPLETED");
        assert_eq!(run.id, "r1");
        // START->COMPLETE span is 4s = 4000ms.
        assert_eq!(run.duration_ms, 4_000);
    }

    #[test]
    fn run_state_start_fail_renders_failed() {
        let model = fold(&[
            run_event("START", 1_000_000, "r1", None, None),
            run_event("FAIL", 2_000_000, "r1", None, None),
        ]);
        let j = job(&model);
        assert_eq!(j.latest_runs[0].state, "FAILED");
    }

    #[test]
    fn run_state_start_only_is_running() {
        let model = fold(&[run_event("START", 1_000_000, "r1", None, None)]);
        let j = job(&model);
        assert_eq!(j.latest_runs[0].state, "RUNNING");
        assert_eq!(j.latest_runs[0].duration_ms, 0);
    }

    #[test]
    fn terminal_state_not_downgraded_by_out_of_order_event() {
        // A late-arriving RUNNING after COMPLETE must not revert the state.
        let model = fold(&[
            run_event("START", 1_000_000, "r1", None, None),
            run_event("COMPLETE", 3_000_000, "r1", None, None),
            run_event("RUNNING", 2_000_000, "r1", None, None),
        ]);
        let j = job(&model);
        assert_eq!(j.latest_runs[0].state, "COMPLETED");
    }

    #[test]
    fn multiple_runs_sorted_newest_first() {
        let model = fold(&[
            run_event("START", 1_000_000, "r1", None, None),
            run_event("COMPLETE", 2_000_000, "r1", None, None),
            run_event("START", 9_000_000, "r2", None, None),
            run_event("FAIL", 10_000_000, "r2", None, None),
        ]);
        let j = job(&model);
        assert_eq!(j.latest_runs.len(), 2);
        assert_eq!(j.latest_runs[0].id, "r2", "newest run first");
        assert_eq!(j.latest_runs[0].state, "FAILED");
        assert_eq!(j.latest_run.as_ref().unwrap().id, "r2");
    }

    #[test]
    fn job_without_run_id_gets_neutral_run() {
        // A pure `job` event (no runId) still yields one neutral run so the UI's
        // latestRuns.reduce has something to fold.
        let model = fold(&[Row {
            kind: "job",
            event_type: None,
            event_time: Some(1_000_000),
            run_id: None,
            job_ns: Some("ns"),
            job_name: Some("job"),
            ds_ns: None,
            ds_name: None,
            inputs: None,
            outputs: None,
            raw: None,
        }]);
        let j = job(&model);
        assert_eq!(j.latest_runs.len(), 1);
        assert_eq!(j.latest_runs[0].state, "COMPLETED");
        assert!(j.latest_runs[0].id.starts_with("norun:"));
    }

    /// A raw event whose `job.facets` carries `documentation` + `tags` (the
    /// shape hydrofoil emits for client-forwarded metadata, ADR 0012).
    const RAW_WITH_META: &str = r#"{"job":{"namespace":"ns","name":"job","facets":{
        "documentation":{"_producer":"p","_schemaURL":"s","description":"Daily rollup."},
        "tags":{"_producer":"p","_schemaURL":"s","tags":[
            {"key":"tier","value":"bronze"},{"key":"adhoc"}]}}}}"#;

    #[test]
    fn job_metadata_folds_from_documentation_and_tags_facets() {
        let mut row = run_event("START", 1_000, "r1", None, None);
        row.raw = Some(RAW_WITH_META);
        let model = fold(&[row, run_event("COMPLETE", 2_000, "r1", None, None)]);
        let j = job(&model);
        assert_eq!(j.description.as_deref(), Some("Daily rollup."));
        assert_eq!(j.tags, vec!["tier:bronze".to_string(), "adhoc".to_string()]);
    }

    #[test]
    fn job_metadata_survives_meta_less_later_event() {
        // The COMPLETE (no job facets) arrives later: it must not erase the
        // metadata the START carried — same latest-wins rule as the edges.
        let mut start = run_event("START", 1_000, "r1", None, None);
        start.raw = Some(RAW_WITH_META);
        let mut complete = run_event("COMPLETE", 2_000, "r1", None, None);
        complete.raw = Some(r#"{"job":{"namespace":"ns","name":"job"}}"#);
        let model = fold(&[start, complete]);
        let j = job(&model);
        assert_eq!(j.description.as_deref(), Some("Daily rollup."));
        assert!(!j.tags.is_empty());
    }

    #[test]
    fn job_without_metadata_keeps_empty_fields() {
        let model = fold(&[run_event("START", 1_000, "r1", None, None)]);
        let j = job(&model);
        assert!(j.description.is_none());
        assert!(j.tags.is_empty());
    }
}
