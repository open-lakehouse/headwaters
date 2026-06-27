//! The [`MutationApplier`] seam: the one place a storage backend is coupled to
//! the projection.
//!
//! A processor emits backend-agnostic [`Mutation`]s; an applier writes them. The
//! applier owns the *canonical* idempotent translation of each mutation kind —
//! the event-time-guarded `ON CONFLICT` upserts, the terminal-rank guard — so
//! that replay-safety lives in one place and a new processor can't break it.
//! A new backend implements this trait and never re-parses events.
//!
//! The transaction is left concrete to each implementation (mirroring the
//! pragmatic `EventSink` style) rather than abstracted behind a GAT — the
//! mutation IR is the load-bearing seam, not the transaction plumbing. The
//! [`PgApplier`](super::backend::postgres::PgApplier) is the only implementation
//! today; it threads a `sqlx` `Transaction` directly.

use super::mutation::Mutation;

/// Translates [`Mutation`]s into backend writes. Implementations apply each
/// mutation idempotently (latest-wins by the mutation's `at`), so applying a
/// batch — or replaying the whole log — is order-insensitive.
///
/// This crate's implementation, [`PgApplier`](super::backend::postgres::PgApplier),
/// exposes `apply(&mut Transaction, &Mutation)` directly rather than through this
/// trait's associated transaction type; the trait documents the contract that
/// any second backend must satisfy.
pub trait MutationApplier {
    /// A human-readable backend identifier (e.g. `"postgres"`).
    fn name(&self) -> &'static str;

    /// The mutation kinds this applier understands. An applier MUST handle every
    /// [`Mutation`] variant or fail loudly — silently dropping a mutation would
    /// make the projection lossy.
    fn handles(&self, _m: &Mutation) -> bool {
        true
    }
}
