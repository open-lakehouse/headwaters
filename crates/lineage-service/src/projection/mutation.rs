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
}
