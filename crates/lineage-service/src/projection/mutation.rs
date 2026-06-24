//! The backend-agnostic mutation IR.
//!
//! A [`Mutation`] describes one change to the read model, independent of any
//! storage backend. [`FacetProcessor`](super::processor::FacetProcessor)s parse
//! a raw event into a `Vec<Mutation>` (pure, no I/O); a backend-specific
//! [`MutationApplier`](super::applier::MutationApplier) translates each variant
//! into writes.
//!
//! Every state-bearing variant carries `at: DateTime<Utc>` — the event time. The
//! applier owns the single canonical latest-wins / terminal-rank guard keyed off
//! `at`, so the projection stays **idempotent and order-insensitive**: replaying
//! the log (or re-applying an event after a crash) reproduces the same tables.
//! A processor must never read backend state — its output is a pure function of
//! the one event — which is what makes that guarantee hold.
//!
//! This is the Phase-0 set: exactly the writes the original `apply_event` fold
//! performed. Richer facet variants (sources, column edges, dataset versions,
//! tags, parent links, …) are added alongside the processors that emit them.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

/// A `{namespace, name}` dataset reference carried on a job's edges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityRef {
    pub namespace: String,
    pub name: String,
}

/// The input/output dataset sets a job event carries. `None` on the enclosing
/// [`Mutation::UpsertJob`] means "this event said nothing about edges" (the
/// applier preserves the stored sets); `Some(empty)` is distinct only in that a
/// processor chooses not to emit edges when the event carries none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobEdges {
    pub inputs: Vec<EntityRef>,
    pub outputs: Vec<EntityRef>,
}

/// One change to the read model. See the module docs for the idempotency
/// contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    /// Note that a namespace was seen at `at` (first/last-seen envelope).
    NoteNamespace { name: String, at: DateTime<Utc> },

    /// Upsert a job. `Option` payloads follow the latest-wins guard: `None`
    /// means "this event carries nothing for this facet", so the applier keeps
    /// what is stored.
    UpsertJob {
        namespace: String,
        name: String,
        at: DateTime<Utc>,
        /// Input/output datasets. `None` when the event carries no edges.
        edges: Option<JobEdges>,
        /// `documentation` job facet.
        description: Option<String>,
        /// `tags` job facet, rendered as `key` / `key:value` strings.
        tags: Option<Vec<String>>,
    },

    /// Fold one run event into the run's state. The applier maps `state`/ranks
    /// and never downgrades a terminal state.
    UpsertRunState {
        run_id: String,
        job_namespace: String,
        job_name: String,
        /// Marquez run state mapped from the OpenLineage `eventType`
        /// (RUNNING/COMPLETED/FAILED/ABORTED), or `None` → treated as NEW.
        state: Option<&'static str>,
        at: DateTime<Utc>,
        is_start: bool,
        is_terminal: bool,
    },

    /// Upsert a dataset, optionally setting its schema fields (latest-wins).
    UpsertDataset {
        namespace: String,
        name: String,
        at: DateTime<Utc>,
        /// `schema` facet columns. `None` when the event carries no schema.
        fields: Option<Vec<JsonValue>>,
    },

    /// A directed lineage edge between two node-id strings (input ds → job,
    /// job → output ds). Add-only.
    UpsertLineageEdge { origin: String, destination: String },

    /// One column (field) of a dataset's schema (from the `schema` facet).
    /// Latest-schema-wins per `(namespace, dataset, field)`.
    UpsertDatasetField {
        namespace: String,
        dataset: String,
        field: String,
        field_type: Option<String>,
        description: Option<String>,
        ordinal: i32,
        at: DateTime<Utc>,
    },

    /// A dataset version snapshot, keyed to the producing run (Marquez's
    /// per-version model). `version` is a deterministic UUID of the schema, so
    /// re-emitting the same schema is a no-op (`ON CONFLICT DO NOTHING`) and
    /// replay stays idempotent. Emitted whenever a schema-bearing event arrives.
    EmitDatasetVersion {
        namespace: String,
        name: String,
        version: uuid::Uuid,
        run_id: Option<String>,
        fields: Vec<JsonValue>,
        at: DateTime<Utc>,
    },

    /// Run metadata folded from run facets (nominalTime, parent, errorMessage).
    /// Each field is `Option`; the applier sets only the present ones, guarded
    /// latest-wins by `at`. Distinct from `UpsertRunState` so a metadata-only
    /// event (e.g. a late facet) doesn't touch the state machine.
    SetRunMeta {
        run_id: String,
        at: DateTime<Utc>,
        nominal_start: Option<DateTime<Utc>>,
        nominal_end: Option<DateTime<Utc>>,
        parent_run_id: Option<String>,
        parent_namespace: Option<String>,
        parent_name: Option<String>,
        error_message: Option<String>,
    },

    /// Job metadata folded from job facets (sourceCodeLocation → location,
    /// jobType, dataSource → source_name, parent job link). Latest-wins by `at`.
    SetJobMeta {
        namespace: String,
        name: String,
        at: DateTime<Utc>,
        location: Option<String>,
        job_type: Option<String>,
        parent_namespace: Option<String>,
        parent_name: Option<String>,
    },

    /// Dataset metadata folded from dataset facets (documentation → description,
    /// dataSource → source_name, lifecycleStateChange DROP → deleted).
    /// Latest-wins by `at`.
    SetDatasetMeta {
        namespace: String,
        name: String,
        at: DateTime<Utc>,
        description: Option<String>,
        source_name: Option<String>,
        deleted: Option<bool>,
    },

    /// A `dataSource` facet → the `sources` catalog (name + connection url).
    UpsertSource {
        name: String,
        connection_url: Option<String>,
        at: DateTime<Utc>,
    },

    /// One column-lineage edge: input field → output field (from the
    /// `columnLineage` facet on an output dataset). Latest-wins per edge key.
    UpsertColumnEdge {
        in_namespace: String,
        in_dataset: String,
        in_field: String,
        out_namespace: String,
        out_dataset: String,
        out_field: String,
        transformation: Option<JsonValue>,
        at: DateTime<Utc>,
    },

    /// A tag in the catalog (idempotent; the description is set if provided).
    UpsertTag {
        tag: String,
        description: Option<String>,
    },

    /// A tag applied to a dataset, a dataset field, or a job — the seed for
    /// downstream tag/PII propagation. Add-only, latest-wins by `at`. These come
    /// from the `tags` facets and from synthetic "fact discovery" tag events
    /// (e.g. a scanner asserting "this column is PII").
    TagAssignment {
        tag: String,
        target: TagTarget,
        at: DateTime<Utc>,
    },
}

/// What a [`Mutation::TagAssignment`] tags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TagTarget {
    Dataset {
        namespace: String,
        name: String,
    },
    DatasetField {
        namespace: String,
        name: String,
        field: String,
    },
    Job {
        namespace: String,
        name: String,
    },
}
