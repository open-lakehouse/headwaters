//! Read layer: a Marquez-compatible REST API served over the Postgres read
//! tables the projection worker maintains.
//!
//! The ingest side ([`crate::writer`]) only appends raw events to `events`; the
//! [`projection`](crate::projection) worker folds those into the normalized
//! `namespaces` / `jobs` / `runs` / `datasets` / `lineage_edges` tables. This
//! module queries those tables with indexed `sqlx` statements ([`queries`]) and
//! shapes the result into Marquez's JSON contract ([`model`]). [`http`] mounts
//! the endpoints the UI needs under `/api/v1`.
//!
//! Reads are eventually consistent: an event is visible here once the projector
//! has folded it (at most one poll interval after ingestion). The lineage graph
//! is a `WITH RECURSIVE` walk over `lineage_edges`; the events feed, run facets,
//! and column-lineage endpoints read the raw `events` log directly.

// The ConnectRPC impl, nodeId parsing, Marquez response shaping, and the SQL
// query layer are internals of the read facade — only `http` (the router) and
// `LineageStore`/`ReadError` below are consumed outside this module.
pub(crate) mod connect;
pub mod http;
pub(crate) mod ids;
pub(crate) mod marquez_compat;
pub(crate) mod queries;

use sqlx::PgPool;

/// Default page size when a request omits `limit` (proto3 unset / missing query
/// param). Shared by the REST and Connect surfaces so both page identically.
pub(crate) const DEFAULT_LIMIT: usize = 100;

/// Hard upper bound on any read page size. An unbounded `limit` would let a
/// single request materialize and serialize an entire table (memory exhaustion /
/// DoS), so both surfaces clamp to this ceiling. Mirrors the `MAX_DEPTH` cap the
/// lineage traversal already applies.
pub(crate) const MAX_LIMIT: usize = 1000;

/// Resolve a requested page size to a safe, surface-consistent value: a missing
/// or non-positive request falls back to `default`, and anything above
/// [`MAX_LIMIT`] is clamped down. Used by both the REST `Pagination` extractor
/// and the Connect `limit_or` so `limit=0`, unset, negative, and oversized
/// requests all resolve the same way regardless of transport.
pub(crate) fn resolve_limit(requested: usize, default: usize) -> usize {
    let n = if requested == 0 { default } else { requested };
    n.min(MAX_LIMIT)
}

/// Errors surfaced by the read layer. The HTTP layer maps these onto status
/// codes (404 for not-found, 500 otherwise).
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("not found: {0}")]
    NotFound(String),
}

impl From<sqlx::Error> for ReadError {
    fn from(e: sqlx::Error) -> Self {
        ReadError::Query(e.to_string())
    }
}

/// Read-only handle over the lineage read tables. Cheap to clone (wraps a
/// connection pool).
#[derive(Clone)]
pub struct LineageStore {
    pub(crate) pool: PgPool,
}

impl LineageStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}
