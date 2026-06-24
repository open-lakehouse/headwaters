//! The [`FacetProcessor`] trait: pure parsers from a raw event to mutations.
//!
//! A processor reads one [`RawEvent`](super::RawEvent) — using the buffa typed
//! facet structs for parsing wherever a facet is involved — and pushes
//! [`Mutation`]s describing the changes it implies. Processors are **pure and
//! synchronous**: no I/O, no database, no reading of current state. Their output
//! must be a function of *this event only*, and every state-bearing mutation
//! must carry the event time (`at`). Those two rules are what make the
//! projection replayable and order-insensitive (see [`mutation`](super::mutation)).
//!
//! Each well-known (or custom) facet gets its own processor under `processors/`;
//! they are composed by the [`ProcessorRegistry`](super::registry::ProcessorRegistry).

use super::RawEvent;
use super::mutation::Mutation;

/// Parses one concern (a well-known facet, a custom facet, or a table-level
/// fold) out of an event into backend-agnostic mutations.
pub trait FacetProcessor: Send + Sync {
    /// Stable identifier for logging / configuration
    /// (e.g. `"core"`, `"schema"`, `"columnLineage"`).
    fn name(&self) -> &'static str;

    /// Emit mutations for `ev` into `out`. A processor that finds nothing
    /// relevant pushes nothing.
    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>);
}
