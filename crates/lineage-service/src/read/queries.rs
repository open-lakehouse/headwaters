//! Marquez read endpoints, served with indexed `sqlx` queries over the
//! projected read tables (and the raw `events` log for the events/facets/
//! column-lineage views).
//!
//! Each method returns the Marquez JSON contract shapes in [`super::model`].
//! The model is maintained incrementally by the [`projection`](crate::projection)
//! worker, so these queries are cheap point/range lookups rather than the
//! full-history fold the old DataFusion reader ran per request.

use chrono::{DateTime, Utc};
use serde_json::{Value as JsonValue, json};
use sqlx::Row;

use super::model::*;
use super::{LineageStore, ReadError};

/// Default and maximum graph traversal depth (hops). Marquez's UI defaults to a
/// depth of 20; we cap to keep the recursion bounded on dense graphs.
const MAX_DEPTH: i32 = 20;

/// Format a timestamp as RFC3339 (Marquez serializes all times as ISO-8601).
fn rfc3339(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339()
}

fn opt_rfc3339(ts: Option<DateTime<Utc>>) -> Option<String> {
    ts.map(rfc3339)
}

impl LineageStore {
    /// `GET /api/v1/namespaces`
    pub async fn namespaces(&self) -> Result<Namespaces, ReadError> {
        let rows = sqlx::query(
            "SELECT name, created_at, updated_at, description FROM namespaces ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;
        let namespaces = rows
            .into_iter()
            .map(|r| Namespace {
                name: r.get("name"),
                created_at: rfc3339(r.get("created_at")),
                updated_at: rfc3339(r.get("updated_at")),
                owner_name: None,
                description: r.get("description"),
                is_hidden: false,
            })
            .collect();
        Ok(Namespaces { namespaces })
    }

    /// `GET /api/v1/namespaces/{ns}/jobs` (when `namespace` is `Some`) and the
    /// global `GET /api/v1/jobs` (when `None`).
    pub async fn jobs(
        &self,
        namespace: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Jobs, ReadError> {
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
                    current_version \
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
        Ok(Jobs {
            jobs,
            total_count: total_count as usize,
        })
    }

    /// `GET /api/v1/namespaces/{ns}/jobs/{job}`
    pub async fn job(&self, namespace: &str, name: &str) -> Result<Job, ReadError> {
        let row = sqlx::query(
            "SELECT namespace, name, created_at, updated_at, description, tags, inputs, outputs, \
                    current_version \
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
    pub async fn job_runs(&self, namespace: &str, name: &str) -> Result<Runs, ReadError> {
        let job = self.job(namespace, name).await?;
        let total_count = job.latest_runs.len();
        Ok(Runs {
            runs: job.latest_runs,
            total_count,
        })
    }

    /// Build a Marquez `Job` from a `jobs` row, loading its runs.
    async fn build_job_from_row(&self, r: &sqlx::postgres::PgRow) -> Result<Job, ReadError> {
        let namespace: String = r.get("namespace");
        let name: String = r.get("name");
        let created_at: DateTime<Utc> = r.get("created_at");
        let updated_at: DateTime<Utc> = r.get("updated_at");
        let description: Option<String> = r.get("description");
        let tags: JsonValue = r.get("tags");
        let inputs: JsonValue = r.get("inputs");
        let outputs: JsonValue = r.get("outputs");
        let current_version: uuid::Uuid = r.get("current_version");

        let node_id = job_node_id(&namespace, &name);
        let updated = rfc3339(updated_at);

        // Runs newest-first.
        let run_rows = sqlx::query(
            "SELECT run_id, state, created_at, updated_at, started_at, ended_at \
             FROM runs WHERE job_namespace = $1 AND job_name = $2 \
             ORDER BY updated_at DESC, created_at DESC",
        )
        .bind(&namespace)
        .bind(&name)
        .fetch_all(&self.pool)
        .await?;

        let latest_runs: Vec<LatestRun> = if run_rows.is_empty() {
            vec![LatestRun::neutral(&node_id, &updated)]
        } else {
            run_rows.iter().map(build_run).collect()
        };
        let latest_run = latest_runs.first().cloned();

        Ok(Job {
            id: EntityId {
                namespace: namespace.clone(),
                name: name.clone(),
            },
            job_type: "BATCH".into(),
            name: name.clone(),
            simple_name: name,
            namespace,
            created_at: rfc3339(created_at),
            updated_at: updated,
            inputs: entity_ids(&inputs),
            outputs: entity_ids(&outputs),
            location: None,
            description,
            latest_run,
            latest_runs,
            tags: string_vec(&tags),
            parent_job_name: None,
            parent_job_uuid: None,
            current_version: current_version.to_string(),
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets` (when `Some`) and global
    /// `GET /api/v1/datasets` (when `None`).
    pub async fn datasets(
        &self,
        namespace: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> Result<Datasets, ReadError> {
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
            "SELECT namespace, name, created_at, updated_at, fields, current_version FROM datasets \
             WHERE ($1::text IS NULL OR namespace = $1) \
             ORDER BY name LIMIT $2 OFFSET $3",
        )
        .bind(namespace)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await?;
        let datasets = rows.iter().map(build_dataset).collect();
        Ok(Datasets {
            datasets,
            total_count: total_count as usize,
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets/{name}`
    pub async fn dataset(&self, namespace: &str, name: &str) -> Result<Dataset, ReadError> {
        let row = sqlx::query(
            "SELECT namespace, name, created_at, updated_at, fields, current_version FROM datasets \
             WHERE namespace = $1 AND name = $2",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ReadError::NotFound(format!("dataset {namespace}/{name}")))?;
        Ok(build_dataset(&row))
    }

    /// `GET /api/v1/search?q=`
    pub async fn search(&self, q: &str, limit: usize) -> Result<Search, ReadError> {
        let pattern = format!("%{}%", q.to_lowercase());
        // Jobs + datasets whose name matches, unioned and sorted by name.
        let job_rows =
            sqlx::query("SELECT namespace, name, updated_at FROM jobs WHERE LOWER(name) LIKE $1")
                .bind(&pattern)
                .fetch_all(&self.pool)
                .await?;
        let ds_rows = sqlx::query(
            "SELECT namespace, name, updated_at FROM datasets WHERE LOWER(name) LIKE $1",
        )
        .bind(&pattern)
        .fetch_all(&self.pool)
        .await?;

        let mut results: Vec<SearchResult> = Vec::new();
        for r in &job_rows {
            let ns: String = r.get("namespace");
            let name: String = r.get("name");
            results.push(SearchResult {
                node_id: job_node_id(&ns, &name),
                name,
                namespace: ns,
                result_type: "JOB".into(),
                updated_at: rfc3339(r.get("updated_at")),
            });
        }
        for r in &ds_rows {
            let ns: String = r.get("namespace");
            let name: String = r.get("name");
            results.push(SearchResult {
                node_id: dataset_node_id(&ns, &name),
                name,
                namespace: ns,
                result_type: "DATASET".into(),
                updated_at: rfc3339(r.get("updated_at")),
            });
        }
        results.sort_by(|a, b| a.name.cmp(&b.name));
        let total_count = results.len();
        results.truncate(limit);
        Ok(Search {
            total_count,
            results,
        })
    }

    /// `GET /api/v1/lineage?nodeId=&depth=`
    ///
    /// Walks `lineage_edges` with a `WITH RECURSIVE` query in both directions
    /// from the seed node up to `depth` hops, then materializes each reached
    /// node with its incident edges.
    pub async fn lineage(&self, node_id: &str, depth: usize) -> Result<LineageGraph, ReadError> {
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
        let edges: Vec<LineageEdge> = edge_rows
            .iter()
            .map(|r| LineageEdge {
                origin: r.get("origin"),
                destination: r.get("destination"),
            })
            .collect();

        let mut graph = Vec::with_capacity(reached.len());
        for id in &reached {
            if let Some(node) = self.build_node(id, &edges).await? {
                graph.push(node);
            }
        }
        Ok(LineageGraph { graph })
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
    /// nodeId, or `None` if it names an entity not in the model.
    async fn build_node(
        &self,
        node_id: &str,
        edges: &[LineageEdge],
    ) -> Result<Option<LineageNode>, ReadError> {
        let Some((kind, namespace, name)) = parse_node_id(node_id) else {
            return Ok(None);
        };
        let (node_type, data) = match kind {
            NodeKind::Job => match self.job(&namespace, &name).await {
                Ok(job) => ("JOB", serde_json::to_value(job).unwrap_or(json!({}))),
                Err(ReadError::NotFound(_)) => return Ok(None),
                Err(e) => return Err(e),
            },
            NodeKind::Dataset => match self.dataset(&namespace, &name).await {
                Ok(ds) => ("DATASET", serde_json::to_value(ds).unwrap_or(json!({}))),
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
        Ok(Some(LineageNode {
            id: node_id.to_string(),
            node_type: node_type.into(),
            data,
            in_edges,
            out_edges,
        }))
    }

    /// `GET /api/v1/events/lineage?limit=&offset=` — newest-first page of the
    /// raw event log; each element is the original OpenLineage document.
    pub async fn events(&self, limit: usize, offset: usize) -> Result<LineageEvents, ReadError> {
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
            .collect();
        Ok(LineageEvents {
            events,
            total_count: total_count as usize,
        })
    }

    /// `GET /api/v1/namespaces/{ns}/datasets/{ds}/versions` — one current
    /// version derived from the dataset (we don't track historical schemas).
    pub async fn dataset_versions(
        &self,
        namespace: &str,
        name: &str,
        limit: usize,
        offset: usize,
    ) -> Result<DatasetVersions, ReadError> {
        let dataset = self.dataset(namespace, name).await?;
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

    /// `GET /api/v1/jobs/runs/{id}/facets` — the run facets carried on the run's
    /// raw events, merged latest-wins. 404 if no event references the run.
    pub async fn run_facets(&self, run_id: &str) -> Result<RunFacets, ReadError> {
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
        Ok(RunFacets {
            run_id: run_id.to_string(),
            facets: JsonValue::Object(facets),
        })
    }

    /// `GET /api/v1/column-lineage?nodeId=` — the dataset column-lineage view,
    /// served from the projected `column_lineage_edges` table (single-hop
    /// upstream, what the UI's dataset column view renders). Unknown datasets /
    /// no column lineage return an empty graph (200, not 404).
    pub async fn column_lineage(&self, node_id: &str) -> Result<ColumnLineageGraph, ReadError> {
        let empty = ColumnLineageGraph { graph: Vec::new() };
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
        let mut in_edges: std::collections::BTreeMap<String, Vec<LineageEdge>> = Default::default();
        let mut out_edges: std::collections::BTreeMap<String, Vec<LineageEdge>> =
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
            let edge = LineageEdge {
                origin: in_id.clone(),
                destination: out_id.clone(),
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

/// Build a Marquez `Run` from a `runs` row.
fn build_run(r: &sqlx::postgres::PgRow) -> LatestRun {
    let started_at: Option<DateTime<Utc>> = r.get("started_at");
    let ended_at: Option<DateTime<Utc>> = r.get("ended_at");
    let duration_ms = match (started_at, ended_at) {
        (Some(s), Some(e)) if e >= s => (e - s).num_milliseconds().max(0) as u64,
        _ => 0,
    };
    LatestRun {
        id: r.get("run_id"),
        created_at: rfc3339(r.get("created_at")),
        updated_at: rfc3339(r.get("updated_at")),
        state: r.get("state"),
        nominal_start_time: None,
        nominal_end_time: None,
        started_at: opt_rfc3339(started_at),
        ended_at: opt_rfc3339(ended_at),
        duration_ms,
    }
}

/// Build a Marquez `Dataset` from a `datasets` row.
fn build_dataset(r: &sqlx::postgres::PgRow) -> Dataset {
    let namespace: String = r.get("namespace");
    let name: String = r.get("name");
    let created_at: DateTime<Utc> = r.get("created_at");
    let updated_at: DateTime<Utc> = r.get("updated_at");
    let fields_json: JsonValue = r.get("fields");
    let current_version: uuid::Uuid = r.get("current_version");
    let fields = fields_json.as_array().cloned().unwrap_or_default();
    let facets = if fields.is_empty() {
        json!({})
    } else {
        json!({ "schema": { "fields": fields } })
    };
    Dataset {
        id: EntityId {
            namespace: namespace.clone(),
            name: name.clone(),
        },
        dataset_type: "DB_TABLE".into(),
        name: name.clone(),
        physical_name: name,
        source_name: namespace.clone(),
        namespace,
        created_at: rfc3339(created_at),
        updated_at: rfc3339(updated_at),
        description: None,
        fields,
        facets,
        tags: Vec::new(),
        deleted: false,
        current_version: current_version.to_string(),
    }
}

/// Parse the `[{namespace,name}]` JSON the projector stores into `EntityId`s.
fn entity_ids(val: &JsonValue) -> Vec<EntityId> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    Some(EntityId {
                        namespace: v.get("namespace")?.as_str()?.to_string(),
                        name: v.get("name")?.as_str()?.to_string(),
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

/// A deterministic version id for a dataset snapshot.
fn stable_version_id(namespace: &str, name: &str, fields: &[JsonValue]) -> String {
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
