//! Postgres [`MutationApplier`]: translates each [`Mutation`] into the
//! `ON CONFLICT` upserts the projection has always used.
//!
//! Every SQL statement here is the one the original `apply_event` fold ran,
//! moved verbatim and keyed off mutation fields instead of re-parsing the event.
//! The event-time guards (`$N >= edges_at OR edges_at IS NULL`), the terminal
//! run-state rank, and the `current_version` refresh-on-change all live here —
//! this is the single canonical place idempotency is enforced.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{Postgres, Transaction};

use crate::projection::applier::MutationApplier;
use crate::projection::mutation::{EntityRef, JobEdges, Mutation};

/// Applies mutations to the Postgres read tables within a caller-provided
/// transaction.
pub struct PgApplier;

impl MutationApplier for PgApplier {
    fn name(&self) -> &'static str {
        "postgres"
    }
}

impl PgApplier {
    /// Apply one mutation within `tx`.
    pub async fn apply(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        m: &Mutation,
    ) -> Result<(), sqlx::Error> {
        match m {
            Mutation::NoteNamespace { name, at } => note_namespace(tx, name, *at).await,
            Mutation::UpsertJob {
                namespace,
                name,
                at,
                edges,
                description,
                tags,
            } => upsert_job(tx, namespace, name, *at, edges, description, tags).await,
            Mutation::UpsertRunState {
                run_id,
                job_namespace,
                job_name,
                state,
                at,
                is_start,
                is_terminal,
            } => {
                fold_run(
                    tx,
                    run_id,
                    job_namespace,
                    job_name,
                    *state,
                    *at,
                    *is_start,
                    *is_terminal,
                )
                .await
            }
            Mutation::UpsertDataset {
                namespace,
                name,
                at,
                fields,
            } => note_dataset(tx, namespace, name, *at, fields.as_ref()).await,
            Mutation::UpsertLineageEdge {
                origin,
                destination,
            } => upsert_edge(tx, origin, destination).await,
            Mutation::UpsertDatasetField {
                namespace,
                dataset,
                field,
                field_type,
                description,
                ordinal,
                at,
            } => {
                upsert_dataset_field(
                    tx,
                    namespace,
                    dataset,
                    field,
                    field_type,
                    description,
                    *ordinal,
                    *at,
                )
                .await
            }
            Mutation::UpsertColumnEdge {
                in_namespace,
                in_dataset,
                in_field,
                out_namespace,
                out_dataset,
                out_field,
                transformation,
                at,
            } => {
                upsert_column_edge(
                    tx,
                    in_namespace,
                    in_dataset,
                    in_field,
                    out_namespace,
                    out_dataset,
                    out_field,
                    transformation,
                    *at,
                )
                .await
            }
        }
    }
}

fn state_rank(state: &str) -> i32 {
    match state {
        "NEW" => 0,
        "RUNNING" => 1,
        "COMPLETED" | "FAILED" | "ABORTED" => 2,
        _ => 0,
    }
}

async fn note_namespace(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    at: DateTime<Utc>,
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
    .bind(at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn ref_to_json(r: &EntityRef) -> JsonValue {
    serde_json::json!({ "namespace": r.namespace, "name": r.name })
}

#[allow(clippy::too_many_arguments)]
async fn upsert_job(
    tx: &mut Transaction<'_, Postgres>,
    ns: &str,
    name: &str,
    at: DateTime<Utc>,
    edges: &Option<JobEdges>,
    description: &Option<String>,
    tags: &Option<Vec<String>>,
) -> Result<(), sqlx::Error> {
    let carries_edges = edges.is_some();
    let has_meta = description.is_some() || tags.is_some();
    let (inputs, outputs) = match edges {
        Some(e) => (
            JsonValue::Array(e.inputs.iter().map(ref_to_json).collect()),
            JsonValue::Array(e.outputs.iter().map(ref_to_json).collect()),
        ),
        None => (JsonValue::Array(vec![]), JsonValue::Array(vec![])),
    };
    let tags_json = JsonValue::Array(
        tags.clone()
            .unwrap_or_default()
            .into_iter()
            .map(JsonValue::String)
            .collect(),
    );

    // `current_version` is refreshed (to a fresh UUIDv7) only when the edges
    // actually change — a new input/output shape — mirroring Marquez's
    // per-version job model; otherwise it is preserved.
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
                           THEN $3 ELSE jobs.meta_at END, \
            current_version = CASE WHEN $8 AND ($3 >= jobs.edges_at OR jobs.edges_at IS NULL) \
                                       AND (jobs.inputs IS DISTINCT FROM $6 \
                                            OR jobs.outputs IS DISTINCT FROM $7) \
                                   THEN uuidv7() ELSE jobs.current_version END",
    )
    .bind(ns)
    .bind(name)
    .bind(at)
    .bind(description)
    .bind(&tags_json)
    .bind(&inputs)
    .bind(&outputs)
    .bind(carries_edges)
    .bind(has_meta)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn fold_run(
    tx: &mut Transaction<'_, Postgres>,
    run_id: &str,
    job_ns: &str,
    job_name: &str,
    state: Option<&'static str>,
    at: DateTime<Utc>,
    is_start: bool,
    is_terminal: bool,
) -> Result<(), sqlx::Error> {
    let new_state = state.unwrap_or("NEW");
    let started_at = if is_start { Some(at) } else { None };
    let ended_at = if is_terminal { Some(at) } else { None };

    // The incoming state's rank ($8) decides whether it supersedes the stored
    // one. A terminal stored state is never downgraded; otherwise the higher
    // rank wins, ties to the incoming event.
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
    .bind(at)
    .bind(started_at)
    .bind(ended_at)
    .bind(state_rank(new_state))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn note_dataset(
    tx: &mut Transaction<'_, Postgres>,
    namespace: &str,
    name: &str,
    at: DateTime<Utc>,
    schema: Option<&Vec<JsonValue>>,
) -> Result<(), sqlx::Error> {
    let has_schema = schema.is_some_and(|f| !f.is_empty());
    let fields_json = JsonValue::Array(schema.cloned().unwrap_or_default());

    // `current_version` is refreshed only when the schema actually changes,
    // mirroring Marquez's per-version dataset model; otherwise preserved.
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
                             THEN $3 ELSE datasets.schema_at END, \
            current_version = CASE WHEN $5 AND ($3 >= datasets.schema_at OR datasets.schema_at IS NULL) \
                                       AND datasets.fields IS DISTINCT FROM $4 \
                                   THEN uuidv7() ELSE datasets.current_version END",
    )
    .bind(namespace)
    .bind(name)
    .bind(at)
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

#[allow(clippy::too_many_arguments)]
async fn upsert_dataset_field(
    tx: &mut Transaction<'_, Postgres>,
    namespace: &str,
    dataset: &str,
    field: &str,
    field_type: &Option<String>,
    description: &Option<String>,
    ordinal: i32,
    at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    // Latest-schema-wins per field: a more recent event replaces type/
    // description/ordinal; an older one is ignored.
    sqlx::query(
        "INSERT INTO dataset_fields \
            (namespace, dataset, field, type, description, ordinal, schema_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) \
         ON CONFLICT (namespace, dataset, field) DO UPDATE SET \
            type        = CASE WHEN $7 >= dataset_fields.schema_at THEN $4 ELSE dataset_fields.type END, \
            description = CASE WHEN $7 >= dataset_fields.schema_at THEN $5 ELSE dataset_fields.description END, \
            ordinal     = CASE WHEN $7 >= dataset_fields.schema_at THEN $6 ELSE dataset_fields.ordinal END, \
            schema_at   = GREATEST(dataset_fields.schema_at, $7)",
    )
    .bind(namespace)
    .bind(dataset)
    .bind(field)
    .bind(field_type)
    .bind(description)
    .bind(ordinal)
    .bind(at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn upsert_column_edge(
    tx: &mut Transaction<'_, Postgres>,
    in_ns: &str,
    in_ds: &str,
    in_field: &str,
    out_ns: &str,
    out_ds: &str,
    out_field: &str,
    transformation: &Option<JsonValue>,
    at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    // Per-output-field latest-wins: a newer event re-declaring an output field's
    // column lineage *replaces* that field's input edges (it does not union with
    // a stale mapping). Delete the output field's older edges first, then upsert
    // this one. This stays replay-safe: deletes are strictly older-than (`<`),
    // so same-`at` edges (the input fields of one facet) coexist, and replaying
    // in any order converges on the max-`at` event's edge set.
    sqlx::query(
        "DELETE FROM column_lineage_edges \
         WHERE out_namespace = $1 AND out_dataset = $2 AND out_field = $3 AND edge_at < $4",
    )
    .bind(out_ns)
    .bind(out_ds)
    .bind(out_field)
    .bind(at)
    .execute(&mut **tx)
    .await?;

    // Insert only if no *newer* event has already declared this output field's
    // lineage — so an out-of-order replay of an older event can't re-add a stale
    // edge (the delete above handles the in-order case; this `NOT EXISTS` guard
    // handles the reverse order, making the fold fully order-insensitive).
    sqlx::query(
        "INSERT INTO column_lineage_edges \
            (in_namespace, in_dataset, in_field, out_namespace, out_dataset, out_field, \
             transformation, edge_at) \
         SELECT $1, $2, $3, $4, $5, $6, $7, $8 \
         WHERE NOT EXISTS ( \
            SELECT 1 FROM column_lineage_edges \
            WHERE out_namespace = $4 AND out_dataset = $5 AND out_field = $6 AND edge_at > $8) \
         ON CONFLICT (in_namespace, in_dataset, in_field, out_namespace, out_dataset, out_field) \
         DO UPDATE SET \
            transformation = CASE WHEN $8 >= column_lineage_edges.edge_at \
                                  THEN $7 ELSE column_lineage_edges.transformation END, \
            edge_at = GREATEST(column_lineage_edges.edge_at, $8)",
    )
    .bind(in_ns)
    .bind(in_ds)
    .bind(in_field)
    .bind(out_ns)
    .bind(out_ds)
    .bind(out_field)
    .bind(transformation)
    .bind(at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}
