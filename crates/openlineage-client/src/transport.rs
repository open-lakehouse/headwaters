//! Pluggable sink for OpenLineage events.
//!
//! Mirrors OpenLineage's own `Transport` SPI naming. The default
//! [`NoopTransport`] is used when no endpoint is configured; [`ConsoleTransport`]
//! is handy for development and tests.

use async_trait::async_trait;

use crate::event::RunEvent;

/// Error returned when a [`Transport`] fails to send an event.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// The event could not be serialized to JSON.
    #[error("failed to serialize lineage event: {0}")]
    Serialize(#[from] serde_json::Error),
    /// A transport-specific delivery failure (e.g. network or backend error).
    #[error("transport error: {0}")]
    Other(String),
}

/// A sink that delivers OpenLineage events to a backend.
#[async_trait]
pub trait Transport: std::fmt::Debug + Send + Sync {
    /// Delivers a single OpenLineage event to the backend.
    ///
    /// # Errors
    /// Returns a [`TransportError`] if the event cannot be serialized or
    /// delivered.
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError>;
}

/// Drops events. The safe default when lineage is not configured.
#[derive(Debug, Default)]
pub struct NoopTransport;

#[async_trait]
impl Transport for NoopTransport {
    async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
        Ok(())
    }
}

/// Logs each event as pretty JSON via `tracing`. For development/tests.
#[derive(Debug, Default)]
pub struct ConsoleTransport;

#[async_trait]
impl Transport for ConsoleTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        let json = serde_json::to_string_pretty(event)?;
        tracing::info!(target: "openlineage", "{json}");
        Ok(())
    }
}
