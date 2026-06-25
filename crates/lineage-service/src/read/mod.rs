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

pub mod connect;
pub mod http;
pub mod ids;
pub mod queries;

use sqlx::PgPool;

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
