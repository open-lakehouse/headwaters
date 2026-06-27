//! Marquez read endpoints, served with indexed `sqlx` queries over the
//! projected read tables (and the raw `events` log for the events/facets/
//! column-lineage views).
//!
//! Each method builds and returns the read-API proto messages in
//! [`crate::proto::headwaters::read::v1`] directly — the single model both the
//! hand-written REST server ([`super::http`]) and the generated ConnectRPC
//! facade ([`super::connect`]) serve. The model is maintained incrementally by
//! the [`projection`](crate::projection) worker, so these queries are cheap
//! point/range lookups rather than a full-history fold per request.
//!
//! Field-for-field these messages mirror the Marquez JSON the web UI consumes;
//! buffa derives proto-JSON serde (camelCase, proto3 empty-field omission), and
//! the opaque/polymorphic bits (`fields`, `facets`, `data`, raw events) are
//! `google.protobuf.Struct` built from the stored JSON via [`struct_from_json`].

use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};
use sqlx::Row;

use super::ids::*;
use super::{LineageStore, ReadError};
use crate::proto::headwaters::read::v1 as pb;
use buffa::{EnumValue, Enumeration, MessageField};
use buffa_types::google::protobuf::Struct as PbStruct;

/// Default and maximum graph traversal depth (hops). Marquez's UI defaults to a
/// depth of 20; we cap to keep the recursion bounded on dense graphs.
const MAX_DEPTH: i32 = 20;

/// Format a timestamp as RFC3339 (Marquez serializes all times as ISO-8601).
fn rfc3339(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339()
}

/// Format an optional timestamp, defaulting to the empty string (proto3 omits
/// empty strings from JSON, matching Marquez's absent-field behavior).
fn opt_rfc3339(ts: Option<DateTime<Utc>>) -> String {
    ts.map(rfc3339).unwrap_or_default()
}

/// Map a stored run-state string (`NEW` | `RUNNING` | `COMPLETED` | `FAILED` |
/// `ABORTED`, written by the projector) to the [`pb::RunState`] enum. An
/// unrecognized value round-trips through `EnumValue::Unknown` rather than
/// failing the query.
fn run_state(state: String) -> EnumValue<pb::RunState> {
    pb::RunState::from_proto_name(&state)
        .map(EnumValue::from)
        .unwrap_or_else(|| EnumValue::from(0))
}

/// Convert a stored JSON value into a `google.protobuf.Struct`. The read DTOs'
/// opaque fields (`facets`, `data`, raw events, schema `fields`) are always JSON
/// objects in practice; a non-object collapses to an empty struct (the wire
/// shape the UI tolerates). Buffa's `Struct` round-trips back through serde as a
/// plain JSON object, so the REST output matches the source JSON.
fn struct_from_json(value: JsonValue) -> PbStruct {
    serde_json::from_value(value).unwrap_or_default()
}

/// A set [`MessageField`] carrying the JSON value as a `Struct` (used for the
/// always-present `facets` / `data` fields the UI dereferences). Even an empty
/// `{}` stays a *set* field so it serializes as `{}` rather than being omitted.
fn struct_field(value: JsonValue) -> MessageField<PbStruct> {
    MessageField::some(struct_from_json(value))
}

/// Convert a JSON array of objects into the repeated-`Struct` schema `fields`.
fn struct_vec(value: &JsonValue) -> Vec<PbStruct> {
    value
        .as_array()
        .map(|arr| arr.iter().cloned().map(struct_from_json).collect())
        .unwrap_or_default()
}

impl LineageStore {
    /// `GET /api/v1/namespaces`
    pub async fn namespaces(&self) -> Result<pb::ListNamespacesResponse, ReadError> {
        let rows = sqlx::query(
            "SELECT name, created_at, updated_at, description FROM namespaces ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        let namespaces = rows
            .into_iter()
            .map(|r| pb::Namespace {
                name: r.get("name"),
                created_at: rfc3339(r.get("created_at")),
                updated_at: rfc3339(r.get("updated_at")),
                owner_name: String::new(),
                description: r
                    .get::<Option<String>, _>("description")
                    .unwrap_or_default(),
                is_hidden: false,
                ..Default::default()
            })
            .collect();
        Ok(pb::ListNamespacesResponse {
            namespaces,
            ..Default::default()
        })
    }

    /// `GET /api/v1/namespaces/{ns}/jobs` (when `namespace` is `Some`) and the
    /// global `GET /api/v1/jobs` (when `None`).
    pub async fn jobs(
        &self,
        namespace: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<pb::ListJobsResponse, ReadError> {
        let total_count: i64 = match namespace {
            Some(ns) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM jobs WHERE namespace = $1")
                    .bind(ns)
                    .fetch_one(&self.pool)
                    .await?
            }
            None => {
                sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
                    .fetch_one(&self.pool)
                    .await?
            }
        };

        let rows = sqlx::query(
            "SELECT namespace, name, created_at, updated_at, description, tags, inputs, outputs, \
                    current_version, location, parent_namespace, parent_name \
             FROM jobs \
             WHERE ($1::text IS NULL OR namespace = $1) \
             ORDER BY name LIMIT $2 OFFSET $3",
        )
        .bind(namespace)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut jobs = Vec::with_capacity(rows.len());
        for r in &rows {
            jobs.push(self.build_job_from_row(r).await?);
        }
        Ok(pb::ListJobsResponse {
            jobs,
            total_count: total_count as i32,
            ..Default::default()
        })
    }

    /// `GET /api/v1/namespaces/{ns}/jobs/{job}`
    pub async fn job(&self, namespace: &str, name: &str) -> Result<pb::JobDetail, ReadError> {
        let row = sqlx::query(
            "SELECT namespace, name, created_at, updated_at, description, tags, inputs, outputs, \
                    current_version, location, parent_namespace, parent_name \
             FROM jobs WHERE namespace = $1 AND name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ReadError::NotFound(format!("job {namespace}/{name}")))?;
        self.build_job_from_row(&row).await
    }

    /// `GET /api/v1/namespaces/{ns}/jobs/{job}/runs`
    pub async fn job_runs(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<pb::ListRunsResponse, ReadError> {
        let job = self.job(namespace, name).await?;
        let total_count = job.latest_runs.len() as i32;
        Ok(pb::ListRunsResponse {
            runs: job.latest_runs,
            total_count,
            ..Default::default()
        })
    }

    /// Build a Marquez `JobDetail` from a `jobs` row, loading its runs.
    async fn build_job_from_row(
        &self,
        r: &sqlx::postgres::PgRow,
    ) -> Result<pb::JobDetail, ReadError> {
        let namespace: String = r.get("namespace");
        let name: String = r.get("name");
        let created_at: DateTime<Utc> = r.get("created_at");
        let updated_at: DateTime<Utc> = r.get("updated_at");
        let description: Option<String> = r.get("description");
        let tags: JsonValue = r.get("tags");
        let inputs: JsonValue = r.get("inputs");
        let outputs: JsonValue = r.get("outputs");
        let current_version: uuid::Uuid = r.get("current_version");
        let location: Option<String> = r.get("location");
        let parent_name: Option<String> = r.get("parent_name");

        let node_id = job_node_id(&namespace, &name);
        let updated = rfc3339(updated_at);

        // Runs newest-first.
        let run_rows = sqlx::query(
            "SELECT run_id, state, created_at, updated_at, started_at, ended_at, \
                    nominal_start, nominal_end \
             FROM runs WHERE job_namespace = $1 AND job_name = $2 \
             ORDER BY updated_at DESC, created_at DESC",
        )
        .bind(&namespace)
        .bind(&name)
        .fetch_all(&self.pool)
        .await?;

        // The dashboard's `latestRuns.reduce(...)` has no initial value and
        // crashes on an empty array, so jobs with no run-typed events still
        // carry one neutral entry.
        let latest_runs: Vec<pb::RunDetail> = if run_rows.is_empty() {
            vec![neutral_run(&node_id, &updated)]
        } else {
            run_rows.iter().map(build_run).collect()
        };
        let latest_run: MessageField<pb::RunDetail> = latest_runs.first().cloned().into();

        Ok(pb::JobDetail {
            id: MessageField::some(pb::EntityId {
                namespace: namespace.clone(),
                name: name.clone(),
                ..Default::default()
            }),
            r#type: pb::JobType::BATCH.into(),
            name: name.clone(),
            simple_name: name,
            namespace,
            created_at: rfc3339(created_at),
            updated_at: updated,
            inputs: entity_ids(&inputs),
            outputs: entity_ids(&outputs),
            location: location.unwrap_or_default(),
            description: description.unwrap_or_default(),
            latest_run,
            latest_runs,
            tags: string_vec(&tags),
            parent_job_name: parent_name.unwrap_or_default(),
            parent_job_uuid: String::new(),
            current_version: current_version.to_string(),
            ..Default::default()
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets` (when `Some`) and global
    /// `GET /api/v1/datasets` (when `None`).
    pub async fn datasets(
        &self,
        namespace: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<pb::ListDatasetsResponse, ReadError> {
        let total_count: i64 = match namespace {
            Some(ns) => {
                sqlx::query_scalar("SELECT COUNT(*) FROM datasets WHERE namespace = $1")
                    .bind(ns)
                    .fetch_one(&self.pool)
                    .await?
            }
            None => {
                sqlx::query_scalar("SELECT COUNT(*) FROM datasets")
                    .fetch_one(&self.pool)
                    .await?
            }
        };
        let rows = sqlx::query(
            "SELECT namespace, name, created_at, updated_at, fields, current_version, \
                    description, source_name, deleted FROM datasets \
             WHERE ($1::text IS NULL OR namespace = $1) \
             ORDER BY name LIMIT $2 OFFSET $3",
        )
        .bind(namespace)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;
        let datasets = rows.iter().map(build_dataset).collect();
        Ok(pb::ListDatasetsResponse {
            datasets,
            total_count: total_count as i32,
            ..Default::default()
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets/{name}`
    pub async fn dataset(&self, namespace: &str, name: &str) -> Result<pb::Dataset, ReadError> {
        let row = sqlx::query(
            "SELECT namespace, name, created_at, updated_at, fields, current_version, \
                    description, source_name, deleted FROM datasets \
             WHERE namespace = $1 AND name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ReadError::NotFound(format!("dataset {namespace}/{name}")))?;
        let mut dataset = build_dataset(&row);
        dataset.tags = self.dataset_tags(namespace, name).await?;
        Ok(dataset)
    }

    /// `GET /api/v1/search?q=`
    ///
    /// `kind` restricts to jobs or datasets (`None` returns both); `namespace`
    /// restricts to one namespace (`None` matches all). Both filters are applied
    /// in SQL so they bound the scan rather than post-filtering.
    pub async fn search(
        &self,
        q: &str,
        limit: usize,
        kind: Option<pb::EntityKind>,
        namespace: Option<&str>,
    ) -> Result<pb::SearchResponse, ReadError> {
        let pattern = format!("%{}%", q.to_lowercase());
        let want_jobs = !matches!(kind, Some(pb::EntityKind::DATASET));
        let want_datasets = !matches!(kind, Some(pb::EntityKind::JOB));

        // `namespace = $2 OR $2 IS NULL` keeps one query shape for both the
        // scoped and the all-namespaces case.
        let select = |table: &str| {
            format!(
                "SELECT namespace, name, updated_at FROM {table} \
                 WHERE LOWER(name) LIKE $1 AND (namespace = $2 OR $2 IS NULL)"
            )
        };

        let mut results: Vec<pb::SearchResult> = Vec::new();
        if want_jobs {
            let job_rows = sqlx::query(&select("jobs"))
                .bind(&pattern)
                .bind(namespace)
                .fetch_all(&self.pool)
                .await?;
            for r in &job_rows {
                let ns: String = r.get("namespace");
                let name: String = r.get("name");
                results.push(pb::SearchResult {
                    node_id: job_node_id(&ns, &name),
                    name,
                    namespace: ns,
                    r#type: pb::EntityKind::JOB.into(),
                    updated_at: rfc3339(r.get("updated_at")),
                    ..Default::default()
                });
            }
        }
        if want_datasets {
            let ds_rows = sqlx::query(&select("datasets"))
                .bind(&pattern)
                .bind(namespace)
                .fetch_all(&self.pool)
                .await?;
            for r in &ds_rows {
                let ns: String = r.get("namespace");
                let name: String = r.get("name");
                results.push(pb::SearchResult {
                    node_id: dataset_node_id(&ns, &name),
                    name,
                    namespace: ns,
                    r#type: pb::EntityKind::DATASET.into(),
                    updated_at: rfc3339(r.get("updated_at")),
                    ..Default::default()
                });
            }
        }
        results.sort_by(|a, b| a.name.cmp(&b.name));
        let total_count = results.len() as i32;
        results.truncate(limit);
        Ok(pb::SearchResponse {
            total_count,
            results,
            ..Default::default()
        })
    }

    /// `GET /api/v1/lineage?nodeId=&depth=`
    ///
    /// Walks `lineage_edges` with a `WITH RECURSIVE` query in both directions
    /// from the seed node up to `depth` hops, then materializes each reached
    /// node with its incident edges.
    pub async fn lineage(
        &self,
        node_id: &str,
        depth: usize,
    ) -> Result<pb::LineageGraph, ReadError> {
        let (seed_kind, seed_ns, seed_name) = parse_node_id(node_id)
            .ok_or_else(|| ReadError::NotFound(format!("malformed nodeId {node_id}")))?;

        // The seed must exist; Marquez 404s an unknown nodeId.
        let seed_known = match seed_kind {
            NodeKind::Job => self.job_exists(&seed_ns, &seed_name).await?,
            NodeKind::Dataset => self.dataset_exists(&seed_ns, &seed_name).await?,
        };
        if !seed_known {
            return Err(ReadError::NotFound(format!("node {node_id}")));
        }

        let depth = (depth as i32).min(MAX_DEPTH);

        // Undirected reachability over the directed edge table.
        let reached: Vec<String> = sqlx::query_scalar(
            "WITH RECURSIVE reach(node, d) AS ( \
                 SELECT $1::text, 0 \
               UNION \
                 SELECT CASE WHEN e.origin = r.node THEN e.destination ELSE e.origin END, r.d + 1 \
                 FROM reach r \
                 JOIN lineage_edges e ON (e.origin = r.node OR e.destination = r.node) \
                 WHERE r.d < $2 \
             ) \
             SELECT DISTINCT node FROM reach",
        )
        .bind(node_id)
        .bind(depth)
        .fetch_all(&self.pool)
        .await?;

        // All edges incident to any reached node.
        let edge_rows = sqlx::query(
            "SELECT origin, destination FROM lineage_edges \
             WHERE origin = ANY($1) OR destination = ANY($1)",
        )
        .bind(&reached)
        .fetch_all(&self.pool)
        .await?;
        let edges: Vec<pb::LineageEdge> = edge_rows
            .iter()
            .map(|r| pb::LineageEdge {
                origin: r.get("origin"),
                destination: r.get("destination"),
                ..Default::default()
            })
            .collect();

        let mut graph = Vec::with_capacity(reached.len());
        for id in &reached {
            if let Some(node) = self.build_node(id, &edges).await? {
                graph.push(node);
            }
        }
        Ok(pb::LineageGraph {
            graph,
            ..Default::default()
        })
    }

    async fn job_exists(&self, namespace: &str, name: &str) -> Result<bool, ReadError> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM jobs WHERE namespace = $1 AND name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_one(&self.pool)
        .await?
            > 0)
    }

    async fn dataset_exists(&self, namespace: &str, name: &str) -> Result<bool, ReadError> {
        Ok(sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM datasets WHERE namespace = $1 AND name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_one(&self.pool)
        .await?
            > 0)
    }

    /// Build a graph node (Job or Dataset payload + incident edges) for a
    /// nodeId, or `None` if it names an entity not in the model. The `data`
    /// payload is the full Job/Dataset message rendered as JSON (the UI's side
    /// panel reads it), carried as a `google.protobuf.Struct`.
    async fn build_node(
        &self,
        node_id: &str,
        edges: &[pb::LineageEdge],
    ) -> Result<Option<pb::LineageNode>, ReadError> {
        let Some((kind, namespace, name)) = parse_node_id(node_id) else {
            return Ok(None);
        };
        let (node_type, data) = match kind {
            NodeKind::Job => match self.job(&namespace, &name).await {
                Ok(job) => (
                    pb::EntityKind::JOB,
                    serde_json::to_value(job).unwrap_or(json!({})),
                ),
                Err(ReadError::NotFound(_)) => return Ok(None),
                Err(e) => return Err(e),
            },
            NodeKind::Dataset => match self.dataset(&namespace, &name).await {
                Ok(ds) => (
                    pb::EntityKind::DATASET,
                    serde_json::to_value(ds).unwrap_or(json!({})),
                ),
                Err(ReadError::NotFound(_)) => return Ok(None),
                Err(e) => return Err(e),
            },
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
        Ok(Some(pb::LineageNode {
            id: node_id.to_string(),
            r#type: node_type.into(),
            data: struct_field(data),
            in_edges,
            out_edges,
            ..Default::default()
        }))
    }

    /// `GET /api/v1/events/lineage?limit=&offset=` — newest-first page of the
    /// raw event log; each element is the original OpenLineage document.
    pub async fn events(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<pb::ListEventsResponse, ReadError> {
        let total_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&self.pool)
            .await?;
        let rows = sqlx::query(
            "SELECT raw FROM events ORDER BY event_time DESC NULLS LAST, seq DESC \
             LIMIT $1 OFFSET $2",
        )
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;
        let events = rows
            .iter()
            .filter_map(|r| r.get::<Option<JsonValue>, _>("raw"))
            .map(struct_from_json)
            .collect();
        Ok(pb::ListEventsResponse {
            events,
            total_count: total_count as i32,
            ..Default::default()
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets/{ds}/versions` — the dataset's
    /// schema history, newest first, from the projected `dataset_versions`
    /// table (one row per distinct schema snapshot, keyed to its producing run).
    /// 404 if the dataset is unknown, like the dataset detail endpoint.
    pub async fn dataset_versions(
        &self,
        namespace: &str,
        name: &str,
        limit: usize,
        offset: usize,
    ) -> Result<pb::ListDatasetVersionsResponse, ReadError> {
        // 404 on an unknown dataset (and reuse its source_name for the message).
        let dataset = self.dataset(namespace, name).await?;

        let total_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM dataset_versions WHERE namespace = $1 AND name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_one(&self.pool)
        .await?;

        let rows = sqlx::query(
            "SELECT version, run_id, fields, created_at FROM dataset_versions \
             WHERE namespace = $1 AND name = $2 \
             ORDER BY created_at DESC, version DESC LIMIT $3 OFFSET $4",
        )
        .bind(namespace)
        .bind(name)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;

        let versions = rows
            .iter()
            .map(|r| {
                let version: uuid::Uuid = r.get("version");
                let version = version.to_string();
                let created_at: DateTime<Utc> = r.get("created_at");
                let fields_json: JsonValue = r.get("fields");
                let fields_arr = fields_json.as_array().cloned().unwrap_or_default();
                let facets = if fields_arr.is_empty() {
                    json!({})
                } else {
                    json!({ "schema": { "fields": fields_arr } })
                };
                pb::DatasetVersion {
                    id: MessageField::some(pb::DatasetVersionId {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        version: version.clone(),
                        ..Default::default()
                    }),
                    r#type: dataset.r#type,
                    name: name.to_string(),
                    physical_name: dataset.physical_name.clone(),
                    created_at: rfc3339(created_at),
                    version,
                    namespace: namespace.to_string(),
                    source_name: dataset.source_name.clone(),
                    fields: struct_vec(&fields_json),
                    tags: Vec::new(),
                    last_modified_at: rfc3339(created_at),
                    description: dataset.description.clone(),
                    facets: struct_field(facets),
                    ..Default::default()
                }
            })
            .collect();

        Ok(pb::ListDatasetVersionsResponse {
            versions,
            total_count: total_count as i32,
            ..Default::default()
        })
    }

    /// `GET /api/v1/jobs/runs/{id}/facets` — the run facets carried on the run's
    /// raw events, merged latest-wins. 404 if no event references the run.
    pub async fn run_facets(&self, run_id: &str) -> Result<pb::RunFacetsResponse, ReadError> {
        let rows = sqlx::query(
            "SELECT raw FROM events WHERE run_id = $1 ORDER BY event_time ASC NULLS LAST, seq ASC",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        if rows.is_empty() {
            return Err(ReadError::NotFound(format!("run {run_id}")));
        }
        let mut facets = serde_json::Map::new();
        for r in &rows {
            let Some(doc): Option<JsonValue> = r.get("raw") else {
                continue;
            };
            if let Some(obj) = doc
                .get("run")
                .and_then(|r| r.get("facets"))
                .and_then(|f| f.as_object())
            {
                for (k, val) in obj {
                    facets.insert(k.clone(), val.clone());
                }
            }
        }
        Ok(pb::RunFacetsResponse {
            run_id: run_id.to_string(),
            facets: struct_field(JsonValue::Object(facets)),
            ..Default::default()
        })
    }

    /// `GET /api/v1/column-lineage?nodeId=` — the dataset column-lineage view,
    /// served from the projected `column_lineage_edges` table (single-hop
    /// upstream, what the UI's dataset column view renders). Unknown datasets /
    /// no column lineage return an empty graph (200, not 404).
    pub async fn column_lineage(&self, node_id: &str) -> Result<pb::LineageGraph, ReadError> {
        let empty = pb::LineageGraph::default();
        let Some((namespace, dataset, field_filter)) = parse_column_lineage_node_id(node_id) else {
            return Ok(empty);
        };

        // Edges terminating at the addressed output dataset (optionally one
        // field). Each row is one input-field → output-field dependency.
        let rows = sqlx::query(
            "SELECT in_namespace, in_dataset, in_field, out_field, transformation \
             FROM column_lineage_edges \
             WHERE out_namespace = $1 AND out_dataset = $2 \
               AND ($3::text IS NULL OR out_field = $3) \
             ORDER BY out_field, in_namespace, in_dataset, in_field",
        )
        .bind(&namespace)
        .bind(&dataset)
        .bind(&field_filter)
        .fetch_all(&self.pool)
        .await?;

        let mut data: std::collections::BTreeMap<String, JsonValue> = Default::default();
        let mut in_edges: std::collections::BTreeMap<String, Vec<pb::LineageEdge>> =
            Default::default();
        let mut out_edges: std::collections::BTreeMap<String, Vec<pb::LineageEdge>> =
            Default::default();
        // Accumulate each output field's inputFields payload (the UI reads it).
        let mut output_inputs: std::collections::BTreeMap<String, Vec<JsonValue>> =
            Default::default();

        for r in &rows {
            let in_ns: String = r.get("in_namespace");
            let in_ds: String = r.get("in_dataset");
            let in_field: String = r.get("in_field");
            let out_field: String = r.get("out_field");
            let transformation: Option<JsonValue> = r.get("transformation");

            let out_id = dataset_field_node_id(&namespace, &dataset, &out_field);
            let in_id = dataset_field_node_id(&in_ns, &in_ds, &in_field);
            let edge = pb::LineageEdge {
                origin: in_id.clone(),
                destination: out_id.clone(),
                ..Default::default()
            };
            in_edges
                .entry(out_id.clone())
                .or_default()
                .push(edge.clone());
            out_edges.entry(in_id.clone()).or_default().push(edge);
            data.entry(in_id).or_insert_with(
                || json!({ "namespace": in_ns, "dataset": in_ds, "field": in_field }),
            );

            let mut input_entry = json!({
                "namespace": in_ns, "name": in_ds, "field": in_field,
            });
            if let Some(t) = transformation {
                input_entry["transformations"] = t;
            }
            output_inputs
                .entry(out_field)
                .or_default()
                .push(input_entry);
        }

        if data.is_empty() {
            return Ok(empty);
        }

        for (out_field, input_fields) in output_inputs {
            let out_id = dataset_field_node_id(&namespace, &dataset, &out_field);
            data.insert(
                out_id,
                json!({
                    "namespace": namespace,
                    "dataset": dataset,
                    "field": out_field,
                    "inputFields": input_fields,
                }),
            );
        }

        let graph = data
            .into_iter()
            .map(|(id, data)| pb::LineageNode {
                in_edges: in_edges.remove(&id).unwrap_or_default(),
                out_edges: out_edges.remove(&id).unwrap_or_default(),
                r#type: pb::EntityKind::DATASET_FIELD.into(),
                data: struct_field(data),
                id,
                ..Default::default()
            })
            .collect();
        Ok(pb::LineageGraph {
            graph,
            ..Default::default()
        })
    }

    /// `GET /api/v1/tags` — the tag catalog.
    pub async fn tags(&self) -> Result<pb::ListTagsResponse, ReadError> {
        let rows = sqlx::query("SELECT name, description FROM tags ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        let tags = rows
            .iter()
            .map(|r| pb::Tag {
                name: r.get("name"),
                description: r
                    .get::<Option<String>, _>("description")
                    .unwrap_or_default(),
                ..Default::default()
            })
            .collect();
        Ok(pb::ListTagsResponse {
            tags,
            ..Default::default()
        })
    }

    /// `GET /api/v1/stats/lineage-events` — event counts bucketed by `period`.
    pub async fn stats_lineage_events(
        &self,
        period: &str,
        limit: usize,
    ) -> Result<pb::StatsResponse, ReadError> {
        let period = normalize_period(period)?;
        // `period` is whitelisted (not user text) so interpolating it into
        // date_trunc is safe; the limit is bound.
        let rows = sqlx::query(&format!(
            "SELECT to_char(date_trunc('{period}', event_time), 'YYYY-MM-DD\"T\"HH24:MI:SSOF') \
                    AS bucket, COUNT(*) AS n \
             FROM events WHERE event_time IS NOT NULL \
             GROUP BY 1 ORDER BY 1 DESC LIMIT $1",
        ))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(stats_response(&rows))
    }

    /// `GET /api/v1/stats/:asset` — first-seen counts for `jobs` or `datasets`,
    /// bucketed by `period`.
    pub async fn stats_asset(
        &self,
        asset: &str,
        period: &str,
        limit: usize,
    ) -> Result<pb::StatsResponse, ReadError> {
        let period = normalize_period(period)?;
        let table = match asset {
            "jobs" | "job" => "jobs",
            "datasets" | "dataset" => "datasets",
            other => {
                return Err(ReadError::NotFound(format!("unknown stats asset {other}")));
            }
        };
        let rows = sqlx::query(&format!(
            "SELECT to_char(date_trunc('{period}', created_at), 'YYYY-MM-DD\"T\"HH24:MI:SSOF') \
                    AS bucket, COUNT(*) AS n \
             FROM {table} GROUP BY 1 ORDER BY 1 DESC LIMIT $1",
        ))
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(stats_response(&rows))
    }

    /// `GET /api/v1/tags/{tag}/downstream` — the dataset fields reachable
    /// downstream from anything currently tagged `tag`.
    ///
    /// A `WITH RECURSIVE` transitive closure over `column_lineage_edges`
    /// (field-granularity), seeded from `tag_assignments`: directly-tagged
    /// fields, plus every field of a tagged dataset. Bounded by `MAX_DEPTH`.
    /// "PII" is just a conventional tag name — nothing is special-cased.
    pub async fn tag_downstream(&self, tag: &str) -> Result<pb::TagPropagation, ReadError> {
        let rows = sqlx::query(
            "WITH RECURSIVE seed(namespace, dataset, field) AS ( \
                 SELECT namespace, name, field FROM tag_assignments \
                 WHERE tag = $1 AND target_type = 'dataset_field' \
               UNION \
                 SELECT ta.namespace, ta.name, f.field \
                 FROM tag_assignments ta \
                 JOIN dataset_fields f ON f.namespace = ta.namespace AND f.dataset = ta.name \
                 WHERE ta.tag = $1 AND ta.target_type = 'dataset' \
             ), \
             reach(namespace, dataset, field, depth) AS ( \
                 SELECT namespace, dataset, field, 0 FROM seed \
               UNION \
                 SELECT e.out_namespace, e.out_dataset, e.out_field, r.depth + 1 \
                 FROM reach r \
                 JOIN column_lineage_edges e \
                   ON e.in_namespace = r.namespace AND e.in_dataset = r.dataset \
                      AND e.in_field = r.field \
                 WHERE r.depth < $2 \
             ) \
             SELECT DISTINCT namespace, dataset, field FROM reach \
             ORDER BY namespace, dataset, field",
        )
        .bind(tag)
        .bind(MAX_DEPTH)
        .fetch_all(&self.pool)
        .await?;

        let fields = rows
            .iter()
            .map(|r| {
                let namespace: String = r.get("namespace");
                let dataset: String = r.get("dataset");
                let field: String = r.get("field");
                let node_id = dataset_field_node_id(&namespace, &dataset, &field);
                pb::TaggedField {
                    namespace,
                    dataset,
                    field,
                    node_id,
                    ..Default::default()
                }
            })
            .collect();
        Ok(pb::TagPropagation {
            tag: tag.to_string(),
            fields,
            ..Default::default()
        })
    }

    /// Tag names assigned to a dataset (whole-dataset assignments only — field
    /// tags are exposed via column lineage / propagation).
    async fn dataset_tags(&self, namespace: &str, name: &str) -> Result<Vec<String>, ReadError> {
        let rows = sqlx::query(
            "SELECT tag FROM tag_assignments \
             WHERE target_type = 'dataset' AND namespace = $1 AND name = $2 ORDER BY tag",
        )
        .bind(namespace)
        .bind(name)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(|r| r.get::<String, _>("tag")).collect())
    }
}

/// Validate a `date_trunc` period against a whitelist (it is interpolated into
/// SQL, so it must not be arbitrary user text).
fn normalize_period(period: &str) -> Result<&'static str, ReadError> {
    match period.to_ascii_lowercase().as_str() {
        "hour" => Ok("hour"),
        "day" => Ok("day"),
        "week" => Ok("week"),
        "month" => Ok("month"),
        other => Err(ReadError::NotFound(format!("unknown period {other}"))),
    }
}

/// Build a `StatsResponse` from `(bucket, n)` rows.
fn stats_response(rows: &[sqlx::postgres::PgRow]) -> pb::StatsResponse {
    pb::StatsResponse {
        buckets: rows
            .iter()
            .map(|r| pb::StatBucket {
                date: r.get("bucket"),
                count: r.get("n"),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// A single neutral run for jobs whose events carry no `run_id` (pure `job`
/// events). The dashboard's `latestRuns.reduce(...)` has no initial value and
/// crashes on an empty array, so we always emit at least this. State is
/// `COMPLETED` (not flagged as failed); `durationMs` 0 renders a minimal bar.
fn neutral_run(job_id: &str, updated_at: &str) -> pb::RunDetail {
    pb::RunDetail {
        id: format!("norun:{job_id}"),
        created_at: updated_at.to_string(),
        updated_at: updated_at.to_string(),
        state: pb::RunState::COMPLETED.into(),
        duration_ms: 0,
        ..Default::default()
    }
}

/// Build a Marquez `RunDetail` from a `runs` row.
fn build_run(r: &sqlx::postgres::PgRow) -> pb::RunDetail {
    let started_at: Option<DateTime<Utc>> = r.get("started_at");
    let ended_at: Option<DateTime<Utc>> = r.get("ended_at");
    let duration_ms = match (started_at, ended_at) {
        (Some(s), Some(e)) if e >= s => (e - s).num_milliseconds().max(0) as u64,
        _ => 0,
    };
    let nominal_start: Option<DateTime<Utc>> = r.get("nominal_start");
    let nominal_end: Option<DateTime<Utc>> = r.get("nominal_end");
    pb::RunDetail {
        id: r.get("run_id"),
        created_at: rfc3339(r.get("created_at")),
        updated_at: rfc3339(r.get("updated_at")),
        state: run_state(r.get("state")),
        nominal_start_time: opt_rfc3339(nominal_start),
        nominal_end_time: opt_rfc3339(nominal_end),
        started_at: opt_rfc3339(started_at),
        ended_at: opt_rfc3339(ended_at),
        duration_ms,
        ..Default::default()
    }
}

/// Build a Marquez `Dataset` from a `datasets` row.
fn build_dataset(r: &sqlx::postgres::PgRow) -> pb::Dataset {
    let namespace: String = r.get("namespace");
    let name: String = r.get("name");
    let created_at: DateTime<Utc> = r.get("created_at");
    let updated_at: DateTime<Utc> = r.get("updated_at");
    let fields_json: JsonValue = r.get("fields");
    let current_version: uuid::Uuid = r.get("current_version");
    let description: Option<String> = r.get("description");
    let source_name: Option<String> = r.get("source_name");
    let deleted: bool = r.get("deleted");
    let fields_arr = fields_json.as_array().cloned().unwrap_or_default();
    let facets = if fields_arr.is_empty() {
        json!({})
    } else {
        json!({ "schema": { "fields": fields_arr } })
    };
    // Prefer the dataSource-facet source name; fall back to the namespace
    // (Marquez derives a default source from the namespace too).
    let source = source_name.unwrap_or_else(|| namespace.clone());
    pb::Dataset {
        id: MessageField::some(pb::EntityId {
            namespace: namespace.clone(),
            name: name.clone(),
            ..Default::default()
        }),
        r#type: pb::DatasetType::DB_TABLE.into(),
        name: name.clone(),
        physical_name: name,
        source_name: source,
        namespace,
        created_at: rfc3339(created_at),
        updated_at: rfc3339(updated_at),
        description: description.unwrap_or_default(),
        fields: struct_vec(&fields_json),
        facets: struct_field(facets),
        tags: Vec::new(),
        deleted,
        current_version: current_version.to_string(),
        ..Default::default()
    }
}

/// Parse the `[{namespace,name}]` JSON the projector stores into `EntityId`s.
fn entity_ids(val: &JsonValue) -> Vec<pb::EntityId> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    Some(pb::EntityId {
                        namespace: v.get("namespace")?.as_str()?.to_string(),
                        name: v.get("name")?.as_str()?.to_string(),
                        ..Default::default()
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Parse a JSON array of strings (job tags).
fn string_vec(val: &JsonValue) -> Vec<String> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}
