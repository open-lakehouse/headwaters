//! Pluggable event-sink abstraction.
//!
//! Each `EventSink` impl owns its own backing store (Postgres today) and
//! consumes a batch of [`EventRow`]s produced from the buffered OpenLineage
//! events. The [`BufferedWriter`](crate::writer::buffered) fans every flushed
//! batch out to one or more sinks; a sink failure is logged and the remaining
//! sinks still run (fail-soft) — there is no synchronous caller to return the
//! error to.

use async_trait::async_trait;

use crate::writer::row::EventRow;

#[async_trait]
pub trait EventSink: Send + Sync {
    /// Stable, human-readable identifier used in logs and error wrapping
    /// (e.g. `"postgres"`).
    fn name(&self) -> &'static str;

    /// Append `rows` to the underlying store.
    ///
    /// Implementations MUST be a no-op for an empty slice — the buffered writer
    /// may call `append` with nothing to flush.
    async fn append(&self, rows: &[EventRow]) -> Result<(), SinkError>;
}

/// Per-sink error envelope. The string payload carries the upstream error's
/// `Display` rendering.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("postgres: {0}")]
    Postgres(String),
}
