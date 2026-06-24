//! Asynchronous buffered writer.
//!
//! Decouples HTTP ingestion from lakehouse writes. HTTP handlers enqueue owned
//! event views onto a bounded channel and return immediately; a background
//! tokio task batches them and flushes to the sinks when the buffer reaches a
//! size threshold OR a flush interval elapses — whichever comes first.
//!
//! This is the in-process successor to the Go `forwarder`
//! (`services/lineage/internal/forwarder/forwarder.go`). Two deliberate
//! differences:
//!   * **Backpressure, not drop.** The Go forwarder dropped events when its
//!     channel was full because a downstream service still buffered them. We
//!     are the terminal writer, so dropping would be silent data loss; instead
//!     `enqueue` awaits a free slot and the pressure propagates to the client.
//!   * **Fail-soft flush.** A sink error is logged and the next sink still
//!     runs — there is no synchronous caller to return the error to, and one
//!     failing sink must not stall the whole pipeline.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::ingest::OwnedEvent;
use crate::lineage::v1::OpenLineageEventView;
use crate::writer::schema::events_to_record_batch;
use crate::writer::sink::TableSink;

/// Tuning knobs for the buffered writer. Defaults mirror the Go forwarder.
#[derive(Debug, Clone, Copy)]
pub struct BufferedWriterConfig {
    pub buffer_size: usize,
    pub flush_interval: Duration,
    pub channel_capacity: usize,
}

impl Default for BufferedWriterConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100,
            flush_interval: Duration::from_millis(500),
            channel_capacity: 1000,
        }
    }
}

/// Cloneable handle that HTTP handlers use to enqueue events. Cheap to clone
/// (wraps an `mpsc::Sender`).
#[derive(Clone)]
pub struct BufferedWriterHandle {
    tx: mpsc::Sender<OwnedEvent>,
}

#[derive(Debug, thiserror::Error)]
#[error("buffered writer is shut down")]
pub struct EnqueueError;

impl BufferedWriterHandle {
    /// Enqueue one event, awaiting a free slot when the channel is full
    /// (backpressure). Errors only when the writer task has stopped.
    pub async fn enqueue(&self, event: OwnedEvent) -> Result<(), EnqueueError> {
        self.tx.send(event).await.map_err(|_| EnqueueError)
    }
}

/// Owns the background flush task and the sole long-lived handle. Dropping all
/// handles closes the channel, which the task treats as a shutdown signal.
pub struct BufferedWriter {
    handle: BufferedWriterHandle,
    task: JoinHandle<()>,
}

impl BufferedWriter {
    /// Spawn the background flush task.
    pub fn spawn(sinks: Vec<Arc<dyn TableSink>>, cfg: BufferedWriterConfig) -> Self {
        let (tx, rx) = mpsc::channel(cfg.channel_capacity);
        let task = tokio::spawn(run(rx, sinks, cfg));
        Self {
            handle: BufferedWriterHandle { tx },
            task,
        }
    }

    pub fn handle(&self) -> BufferedWriterHandle {
        self.handle.clone()
    }

    /// Close the channel and await a final drain.
    ///
    /// The task only exits once *every* sender is dropped, so all cloned
    /// [`BufferedWriterHandle`]s (e.g. the one in axum state) must be dropped
    /// before calling this — otherwise the channel never closes and the await
    /// blocks. In `main.rs` this is guaranteed by stopping the HTTP server (and
    /// thus dropping its state) before `shutdown`.
    pub async fn shutdown(self) {
        drop(self.handle);
        let _ = self.task.await;
    }
}

async fn run(
    mut rx: mpsc::Receiver<OwnedEvent>,
    sinks: Vec<Arc<dyn TableSink>>,
    cfg: BufferedWriterConfig,
) {
    let mut interval = tokio::time::interval(cfg.flush_interval);
    // If a flush takes longer than the interval, don't fire a burst of catch-up
    // ticks afterwards — just resume the cadence.
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first tick fires immediately; consume it so we don't flush an empty
    // buffer on startup.
    interval.tick().await;

    let mut buf: Vec<OwnedEvent> = Vec::with_capacity(cfg.buffer_size);

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(event) => {
                    buf.push(event);
                    if buf.len() >= cfg.buffer_size {
                        flush(&sinks, &mut buf).await;
                        interval.reset();
                    }
                }
                // All senders dropped: drain whatever is buffered and exit.
                None => {
                    flush(&sinks, &mut buf).await;
                    return;
                }
            },
            _ = interval.tick() => {
                flush(&sinks, &mut buf).await;
            }
        }
    }
}

/// Convert the buffered events to one Arrow batch and fan it out to every sink.
/// Clears the buffer unconditionally — a conversion or sink failure drops that
/// flush's events (logged) rather than wedging the pipeline.
async fn flush(sinks: &[Arc<dyn TableSink>], buf: &mut Vec<OwnedEvent>) {
    if buf.is_empty() {
        return;
    }
    let count = buf.len();

    // `events_to_record_batch` takes a slice of borrowed-lifetime views;
    // reborrow each owned view and clone it into an owned-view value. The view
    // is a cheap handle over shared `Bytes`, so the clone is shallow.
    let views: Vec<OpenLineageEventView<'_>> = buf.iter().map(|ev| ev.reborrow().clone()).collect();

    match events_to_record_batch(&views) {
        Ok(batch) => {
            for sink in sinks {
                if let Err(e) = sink.append(batch.clone()).await {
                    tracing::error!("{} flush failed ({count} events): {e}", sink.name());
                }
            }
        }
        Err(e) => {
            tracing::error!("schema conversion failed, dropping {count} events: {e}");
        }
    }

    drop(views);
    buf.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::convert_event;
    use crate::writer::sink::SinkError;
    use deltalake::arrow::array::RecordBatch;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A sink that records how many rows and how many flush calls it has seen.
    struct CountingSink {
        rows: AtomicUsize,
        flushes: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl TableSink for CountingSink {
        fn name(&self) -> &'static str {
            "counting"
        }
        async fn append(&self, batch: RecordBatch) -> Result<(), SinkError> {
            if batch.num_rows() == 0 {
                return Ok(());
            }
            self.rows.fetch_add(batch.num_rows(), Ordering::SeqCst);
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn event(run_id: &str) -> OwnedEvent {
        let json = format!(
            r#"{{"eventType":"COMPLETE","eventTime":"2026-04-28T19:30:00.000Z",
                "producer":"p","run":{{"runId":"{run_id}"}},
                "job":{{"namespace":"ns","name":"j"}}}}"#
        );
        convert_event(json.as_bytes()).unwrap()
    }

    fn counting() -> (Arc<CountingSink>, Vec<Arc<dyn TableSink>>) {
        let sink = Arc::new(CountingSink {
            rows: AtomicUsize::new(0),
            flushes: AtomicUsize::new(0),
        });
        let sinks: Vec<Arc<dyn TableSink>> = vec![sink.clone()];
        (sink, sinks)
    }

    #[tokio::test]
    async fn flushes_when_buffer_size_reached() {
        let (sink, sinks) = counting();
        // Large interval so only the size trigger can fire within the test.
        let writer = BufferedWriter::spawn(
            sinks,
            BufferedWriterConfig {
                buffer_size: 3,
                flush_interval: Duration::from_secs(3600),
                channel_capacity: 16,
            },
        );
        let h = writer.handle();
        for i in 0..3 {
            h.enqueue(event(&format!("r{i}"))).await.unwrap();
        }
        // Give the task a moment to observe the third event and flush.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(sink.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(sink.rows.load(Ordering::SeqCst), 3);
        // Drop the handle before shutting down so the channel can close.
        drop(h);
        writer.shutdown().await;
    }

    #[tokio::test]
    async fn flushes_on_interval_below_buffer_size() {
        let (sink, sinks) = counting();
        let writer = BufferedWriter::spawn(
            sinks,
            BufferedWriterConfig {
                buffer_size: 100,
                flush_interval: Duration::from_millis(50),
                channel_capacity: 16,
            },
        );
        let h = writer.handle();
        h.enqueue(event("r0")).await.unwrap();
        h.enqueue(event("r1")).await.unwrap();
        // Wait past the interval; the time trigger should flush the 2 events.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(sink.flushes.load(Ordering::SeqCst) >= 1);
        assert_eq!(sink.rows.load(Ordering::SeqCst), 2);
        drop(h);
        writer.shutdown().await;
    }

    #[tokio::test]
    async fn drains_remaining_on_shutdown() {
        let (sink, sinks) = counting();
        let writer = BufferedWriter::spawn(
            sinks,
            BufferedWriterConfig {
                buffer_size: 100,
                flush_interval: Duration::from_secs(3600),
                channel_capacity: 16,
            },
        );
        let h = writer.handle();
        h.enqueue(event("r0")).await.unwrap();
        h.enqueue(event("r1")).await.unwrap();
        drop(h);
        // shutdown drops the last sender, closing the channel and forcing a
        // final drain flush.
        writer.shutdown().await;
        assert_eq!(sink.rows.load(Ordering::SeqCst), 2);
    }
}
