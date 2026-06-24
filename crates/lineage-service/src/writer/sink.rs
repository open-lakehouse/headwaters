//! Pluggable lakehouse sink abstraction.
//!
//! Each `TableSink` impl owns its own table handle (Delta, Unity, ...) and
//! consumes the same shared Arrow schema produced by `events_to_record_batch`.
//! The [`BufferedWriter`](crate::writer::buffered) fans every flushed
//! `RecordBatch` out to one or more sinks; a sink failure is logged and the
//! remaining sinks still run (fail-soft).

use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;

#[async_trait]
pub trait TableSink: Send + Sync {
    /// Stable, human-readable identifier used in logs and error wrapping
    /// (e.g. `"delta"`, `"unity"`).
    fn name(&self) -> &'static str;

    /// Append `batch` to the underlying table.
    ///
    /// Implementations MUST be a no-op for empty batches (`num_rows() == 0`)
    /// to preserve the historical behavior of `DeltaWriter::append` — the
    /// service layer relies on this so that an all-`event=None` `WriteBatch`
    /// reports `written: 0` without creating an empty Delta snapshot.
    async fn append(&self, batch: RecordBatch) -> Result<(), SinkError>;
}

/// Per-sink error envelope. The string payload carries the upstream error's
/// `Display` rendering — sinks are free to map any internal failure into the
/// matching variant.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("delta: {0}")]
    Delta(String),

    #[error("unity: {0}")]
    Unity(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingSink {
        name: &'static str,
        count: AtomicUsize,
        fail: bool,
    }

    #[async_trait]
    impl TableSink for CountingSink {
        fn name(&self) -> &'static str {
            self.name
        }
        async fn append(&self, _batch: RecordBatch) -> Result<(), SinkError> {
            if self.fail {
                return Err(SinkError::Delta("boom".into()));
            }
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn empty_batch() -> RecordBatch {
        use crate::writer::schema::arrow_schema;
        RecordBatch::new_empty(arrow_schema())
    }

    #[tokio::test]
    async fn dyn_dispatch_calls_every_sink() {
        let a = Arc::new(CountingSink {
            name: "a",
            count: AtomicUsize::new(0),
            fail: false,
        });
        let b = Arc::new(CountingSink {
            name: "b",
            count: AtomicUsize::new(0),
            fail: false,
        });
        let sinks: Vec<Arc<dyn TableSink>> = vec![a.clone(), b.clone()];

        for s in &sinks {
            s.append(empty_batch()).await.unwrap();
        }

        assert_eq!(a.count.load(Ordering::SeqCst), 1);
        assert_eq!(b.count.load(Ordering::SeqCst), 1);
        assert_eq!(sinks[0].name(), "a");
        assert_eq!(sinks[1].name(), "b");
    }

    #[tokio::test]
    async fn error_variants_render_with_prefix() {
        let e = SinkError::Delta("foo".into());
        assert_eq!(e.to_string(), "delta: foo");
        let e = SinkError::Unity("bar".into());
        assert_eq!(e.to_string(), "unity: bar");
    }
}
