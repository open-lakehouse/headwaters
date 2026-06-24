//! Per-event fold: turn one raw event into upserts on the read tables.
//!
//! This is the SQL port of the in-memory model fold the old DataFusion reader
//! ran on every request. The same rules apply, now incrementally and durably:
//!
//! - **Run state** maps from `event_type`; a terminal state (COMPLETED/FAILED/
//!   ABORTED) never downgrades to a non-terminal one regardless of arrival
//!   order. START/terminal event times feed the run's duration.
//! - **Job edges** (input/output datasets) union latest-event-wins: an event
//!   carrying datasets replaces the sets only when it's at least as recent as
//!   the edges we already have, so an empty terminal event doesn't erase them.
//! - **Job metadata** (description/tags from the documentation/tags job facets)
//!   follows the same latest-wins guard.
//! - **Dataset schema** (columns from the schema facet) is latest-schema-wins;
//!   an event without a schema facet never clears one.
//!
//! All statements are `ON CONFLICT` upserts guarded by the event time, so the
//! fold is order-insensitive and idempotent (safe to replay).

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{Postgres, Transaction};

use super::RawEvent;
use crate::read::model::{dataset_node_id, job_node_id};

/// Apply one event's effect to the read tables within `tx`.
pub(super) async fn apply_event(
    tx: &mut Transaction<'_, Postgres>,
    ev: &RawEvent,
) -> Result<(), sqlx::Error> {
    // A missing event_time is "unknown"; fall back to the epoch so the
    // latest-wins guards still have a total order (matches the old fold, which
    // used 0 for unknown timestamps).
    let ts = ev
        .event_time
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0));

    match ev.event_kind.as_str() {
        "run" | "job" => apply_run_or_job(tx, ev, ts).await,
        "dataset" => {
            if let (Some(ns), Some(name)) = (&ev.dataset_namespace, &ev.dataset_name) {
                note_dataset(tx, ns, name, ts, None).await?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn apply_run_or_job(
    tx: &mut Transaction<'_, Postgres>,
    ev: &RawEvent,
    ts: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let (Some(ns), Some(name)) = (&ev.job_namespace, &ev.job_name) else {
        return Ok(());
    };

    let in_refs = parse_refs(&ev.inputs);
    let out_refs = parse_refs(&ev.outputs);
    let carries_edges = !in_refs.is_empty() || !out_refs.is_empty();
    let (description, tags) = parse_job_meta(&ev.raw);
    let has_meta = description.is_some() || !tags.is_empty();

    // Namespaces seen on either side.
    note_namespace(tx, ns, ts).await?;

    // Upsert the job row. first_seen/last_seen track the event-time envelope;
    // edges and metadata are replaced only by an event at least as recent as
    // what produced the current values (latest-event-wins). NULLIF keeps the
    // existing edges_at/meta_at when this event carries nothing for them.
    let inputs_json = JsonValue::Array(in_refs.iter().map(ref_to_json).collect());
    let outputs_json = JsonValue::Array(out_refs.iter().map(ref_to_json).collect());
    let tags_json = JsonValue::Array(tags.into_iter().map(JsonValue::String).collect());

    sqlx::query(
        "INSERT INTO jobs (namespace, name, created_at, updated_at, \
                           description, tags, inputs, outputs, edges_at, meta_at) \
         VALUES ($1, $2, $3, $3, \
                 CASE WHEN $9 THEN $4 ELSE NULL END, \
                 CASE WHEN $9 THEN $5 ELSE '[]'::jsonb END, \
                 CASE WHEN $8 THEN $6 ELSE '[]'::jsonb END, \
                 CASE WHEN $8 THEN $7 ELSE '[]'::jsonb END, \
                 CASE WHEN $8 THEN $3 ELSE NULL END, \
                 CASE WHEN $9 THEN $3 ELSE NULL END) \
         ON CONFLICT (namespace, name) DO UPDATE SET \
            created_at = LEAST(jobs.created_at, EXCLUDED.created_at), \
            updated_at = GREATEST(jobs.updated_at, EXCLUDED.updated_at), \
            inputs  = CASE WHEN $8 AND ($3 >= jobs.edges_at OR jobs.edges_at IS NULL) \
                           THEN $6 ELSE jobs.inputs END, \
            outputs = CASE WHEN $8 AND ($3 >= jobs.edges_at OR jobs.edges_at IS NULL) \
                           THEN $7 ELSE jobs.outputs END, \
            edges_at = CASE WHEN $8 AND ($3 >= jobs.edges_at OR jobs.edges_at IS NULL) \
                            THEN $3 ELSE jobs.edges_at END, \
            description = CASE WHEN $9 AND ($3 >= jobs.meta_at OR jobs.meta_at IS NULL) \
                               THEN $4 ELSE jobs.description END, \
            tags = CASE WHEN $9 AND ($3 >= jobs.meta_at OR jobs.meta_at IS NULL) \
                        THEN $5 ELSE jobs.tags END, \
            meta_at = CASE WHEN $9 AND ($3 >= jobs.meta_at OR jobs.meta_at IS NULL) \
                           THEN $3 ELSE jobs.meta_at END",
    )
    .bind(ns)
    .bind(name)
    .bind(ts)
    .bind(&description)
    .bind(&tags_json)
    .bind(&inputs_json)
    .bind(&outputs_json)
    .bind(carries_edges)
    .bind(has_meta)
    .execute(&mut **tx)
    .await?;

    // Per-run state.
    if let Some(rid) = &ev.run_id {
        fold_run(tx, rid, ns, name, ev.event_type.as_deref(), ts).await?;
    }

    // Datasets implied by the edges. Output datasets may carry a schema facet.
    let out_schemas = parse_output_schemas(&ev.raw);
    for r in &in_refs {
        note_dataset(tx, &r.namespace, &r.name, ts, None).await?;
        note_namespace(tx, &r.namespace, ts).await?;
    }
    for r in &out_refs {
        let schema = out_schemas.get(&(r.namespace.clone(), r.name.clone()));
        note_dataset(tx, &r.namespace, &r.name, ts, schema).await?;
        note_namespace(tx, &r.namespace, ts).await?;
    }

    // Edges: input dataset -> job, job -> output dataset (latest-wins set means
    // we re-derive on every edge-bearing event; older edges remain until
    // superseded, matching the union semantics of the old fold).
    if carries_edges {
        let job_node = job_node_id(ns, name);
        for r in &in_refs {
            upsert_edge(tx, &dataset_node_id(&r.namespace, &r.name), &job_node).await?;
        }
        for r in &out_refs {
            upsert_edge(tx, &job_node, &dataset_node_id(&r.namespace, &r.name)).await?;
        }
    }

    Ok(())
}

/// Marquez run states, ordered so a later terminal event can't be downgraded by
/// a stray earlier-typed event.
fn state_rank(state: &str) -> i32 {
    match state {
        "NEW" => 0,
        "RUNNING" => 1,
        "COMPLETED" | "FAILED" | "ABORTED" => 2,
        _ => 0,
    }
}

/// Map an OpenLineage `eventType` (case-insensitive) to a Marquez run state.
fn event_type_to_state(et: &str) -> Option<&'static str> {
    match et.to_ascii_uppercase().as_str() {
        "START" | "RUNNING" => Some("RUNNING"),
        "COMPLETE" => Some("COMPLETED"),
        "FAIL" => Some("FAILED"),
        "ABORT" => Some("ABORTED"),
        _ => None,
    }
}

async fn fold_run(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &str,
    job_ns: &str,
    job_name: &str,
    event_type: Option<&str>,
    ts: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let new_state = event_type.and_then(event_type_to_state).unwrap_or("NEW");
    let is_start = event_type.is_some_and(|et| et.eq_ignore_ascii_case("START"));
    let is_terminal = state_rank(new_state) == 2;

    // started_at / ended_at only set on the relevant event types.
    let started_at = if is_start { Some(ts) } else { None };
    let ended_at = if is_terminal { Some(ts) } else { None };

    // The incoming state's rank ($8) decides whether it supersedes the stored
    // one. The stored rank is recomputed inline from `runs.state` (a terminal
    // stored state — rank 2 — is never downgraded; otherwise the higher rank
    // wins, with ties going to the incoming event).
    sqlx::query(
        "INSERT INTO runs (run_id, job_namespace, job_name, state, \
                           created_at, updated_at, started_at, ended_at) \
         VALUES ($1, $2, $3, $4, $5, $5, $6, $7) \
         ON CONFLICT (run_id) DO UPDATE SET \
            created_at = LEAST(runs.created_at, EXCLUDED.created_at), \
            updated_at = GREATEST(runs.updated_at, EXCLUDED.updated_at), \
            started_at = COALESCE(EXCLUDED.started_at, runs.started_at), \
            ended_at   = COALESCE(EXCLUDED.ended_at, runs.ended_at), \
            state = CASE \
                WHEN $8 = 2 THEN EXCLUDED.state \
                WHEN runs.state IN ('COMPLETED', 'FAILED', 'ABORTED') THEN runs.state \
                WHEN $8 >= (CASE runs.state WHEN 'RUNNING' THEN 1 ELSE 0 END) \
                     THEN EXCLUDED.state \
                ELSE runs.state END",
    )
    .bind(run_id)
    .bind(job_ns)
    .bind(job_name)
    .bind(new_state)
    .bind(ts)
    .bind(started_at)
    .bind(ended_at)
    .bind(state_rank(new_state))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn note_namespace(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    ts: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    if name.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "INSERT INTO namespaces (name, created_at, updated_at) VALUES ($1, $2, $2) \
         ON CONFLICT (name) DO UPDATE SET \
            created_at = LEAST(namespaces.created_at, EXCLUDED.created_at), \
            updated_at = GREATEST(namespaces.updated_at, EXCLUDED.updated_at)",
    )
    .bind(name)
    .bind(ts)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Upsert a dataset, optionally setting its schema fields (latest-schema-wins).
async fn note_dataset(
    tx: &mut Transaction<'_, Postgres>,
    namespace: &str,
    name: &str,
    ts: DateTime<Utc>,
    schema: Option<&Vec<JsonValue>>,
) -> Result<(), sqlx::Error> {
    let has_schema = schema.is_some_and(|f| !f.is_empty());
    let fields_json = JsonValue::Array(schema.cloned().unwrap_or_default());

    sqlx::query(
        "INSERT INTO datasets (namespace, name, created_at, updated_at, fields, schema_at) \
         VALUES ($1, $2, $3, $3, \
                 CASE WHEN $5 THEN $4 ELSE '[]'::jsonb END, \
                 CASE WHEN $5 THEN $3 ELSE NULL END) \
         ON CONFLICT (namespace, name) DO UPDATE SET \
            created_at = LEAST(datasets.created_at, EXCLUDED.created_at), \
            updated_at = GREATEST(datasets.updated_at, EXCLUDED.updated_at), \
            fields = CASE WHEN $5 AND ($3 >= datasets.schema_at OR datasets.schema_at IS NULL) \
                          THEN $4 ELSE datasets.fields END, \
            schema_at = CASE WHEN $5 AND ($3 >= datasets.schema_at OR datasets.schema_at IS NULL) \
                             THEN $3 ELSE datasets.schema_at END",
    )
    .bind(namespace)
    .bind(name)
    .bind(ts)
    .bind(&fields_json)
    .bind(has_schema)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn upsert_edge(
    tx: &mut Transaction<'_, Postgres>,
    origin: &str,
    destination: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO lineage_edges (origin, destination) VALUES ($1, $2) \
         ON CONFLICT (origin, destination) DO NOTHING",
    )
    .bind(origin)
    .bind(destination)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// --- JSON parsing helpers (ported from the old read::queries fold) ---

/// A dataset reference parsed from `inputs` / `outputs` JSON.
struct Ref {
    namespace: String,
    name: String,
}

fn ref_to_json(r: &Ref) -> JsonValue {
    serde_json::json!({ "namespace": r.namespace, "name": r.name })
}

fn parse_refs(val: &Option<JsonValue>) -> Vec<Ref> {
    let Some(JsonValue::Array(arr)) = val else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|v| {
            Some(Ref {
                namespace: v.get("namespace")?.as_str()?.to_string(),
                name: v.get("name")?.as_str()?.to_string(),
            })
        })
        .collect()
}

/// Extract the job description (from the `documentation` job facet) and tags
/// (from the `tags` job facet, rendered as `key` / `key:value`).
fn parse_job_meta(raw: &Option<JsonValue>) -> (Option<String>, Vec<String>) {
    let Some(event) = raw else {
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

/// Extract per-output-dataset schema fields from an event's raw JSON:
/// `outputs[].facets.schema.fields`, keyed by `(namespace, name)`.
fn parse_output_schemas(
    raw: &Option<JsonValue>,
) -> std::collections::HashMap<(String, String), Vec<JsonValue>> {
    let mut out = std::collections::HashMap::new();
    let Some(event) = raw else { return out };
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
