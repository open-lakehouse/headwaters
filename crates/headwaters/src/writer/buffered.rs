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
use crate::writer::row::event_to_row;
use crate::writer::sink::EventSink;

/// Tuning knobs for the buffered writer. Defaults mirror the Go forwarder.
#[derive(Debug, Clone, Copy)]
pub struct BufferedWriterConfig {
    /// Flush once this many events are buffered.
    pub buffer_size: usize,
    /// Flush at least this often, even below `buffer_size`.
    pub flush_interval: Duration,
    /// Bounded ingestion channel depth; enqueue applies backpressure once full.
    pub channel_capacity: usize,
    /// How many times a flush retries a failing sink (with exponential backoff)
    /// before giving up on this attempt and re-buffering the batch for the next
    /// trigger. Total tries = `flush_max_retries + 1`.
    pub flush_max_retries: u32,
    /// Base backoff between flush retries; doubled each attempt.
    pub flush_retry_backoff: Duration,
}

impl Default for BufferedWriterConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100,
            flush_interval: Duration::from_millis(500),
            channel_capacity: 1000,
            flush_max_retries: 3,
            flush_retry_backoff: Duration::from_millis(100),
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
    ///
    /// Currently only a single sink is supported: [`flush`] re-sends the whole
    /// batch to every sink on retry, which would double-insert into a sink that
    /// already accepted the batch. Adding a second sink requires per-sink success
    /// tracking first — this assert guards the invariant in debug builds.
    pub fn spawn(sinks: Vec<Arc<dyn EventSink>>, cfg: BufferedWriterConfig) -> Self {
        debug_assert!(
            sinks.len() <= 1,
            "multiple sinks need per-sink retry tracking to avoid double-inserts; see flush()"
        );
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

    /// Close the channel and await a final drain, bounded by `drain_timeout`.
    ///
    /// The task only exits once *every* sender is dropped, so all cloned
    /// [`BufferedWriterHandle`]s (e.g. the one in axum state) must be dropped
    /// before calling this — otherwise the channel never closes and the await
    /// blocks. In `main.rs` this is guaranteed by stopping the HTTP server (and
    /// thus dropping its state) before `shutdown`.
    ///
    /// The final drain retries a failing sink (see [`flush`]), so a dead
    /// Postgres would otherwise wedge the drain forever. `drain_timeout` caps the
    /// wait: on expiry the task is aborted and the still-buffered events are lost,
    /// but the process can exit instead of hanging. Pick a timeout long enough to
    /// absorb a brief blip but short enough to satisfy the orchestrator's
    /// termination grace period.
    pub async fn shutdown(self, drain_timeout: Duration) {
        drop(self.handle);
        let mut task = self.task;
        match tokio::time::timeout(drain_timeout, &mut task).await {
            // Drained (or returned) within the deadline.
            Ok(Ok(())) => {}
            // The task panicked — surface it rather than report a clean drain.
            Ok(Err(e)) => {
                tracing::error!("buffered writer task ended abnormally during drain: {e}");
            }
            // Timed out: `timeout` only stops awaiting, so abort to actually stop
            // the task instead of leaking it until process exit.
            Err(_) => {
                task.abort();
                tracing::error!(
                    ?drain_timeout,
                    "buffered writer drain timed out; aborted with events still buffered"
                );
            }
        }
    }
}

async fn run(
    mut rx: mpsc::Receiver<OwnedEvent>,
    sinks: Vec<Arc<dyn EventSink>>,
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
                        // Only reset the cadence when the flush actually drained
                        // the buffer. On a failed flush the batch is retained, so
                        // resetting would push the interval-tick retry a full
                        // interval out and starve it while events keep arriving.
                        if flush(&sinks, &mut buf, &cfg).await {
                            interval.reset();
                        }
                    }
                }
                // All senders dropped: drain whatever is buffered and exit. The
                // per-flush retry already backs off; if the final flush still
                // fails the events are retained but we must not loop forever on a
                // dead sink, so the caller (`shutdown`) bounds this with a
                // timeout. Retry until the buffer drains or we're cancelled.
                None => {
                    while !flush(&sinks, &mut buf, &cfg).await {
                        tokio::time::sleep(cfg.flush_retry_backoff).await;
                    }
                    return;
                }
            },
            _ = interval.tick() => {
                flush(&sinks, &mut buf, &cfg).await;
            }
        }
    }
}

/// Convert the buffered events to `EventRow`s and fan them out to every sink,
/// retrying a failing sink with exponential backoff. **The buffer is cleared
/// only when the batch was durably written to every sink.** If a sink still
/// fails after all retries, the events are *retained* in `buf` so the next flush
/// trigger re-attempts them — we are the terminal source-of-truth writer, so a
/// transient outage must not silently drop events ("backpressure, not drop").
///
/// Returns `true` if the batch was written and the buffer cleared, `false` if it
/// was retained for retry.
///
/// NOTE: with the single configured sink this re-sends the whole batch on retry,
/// which is safe. If a second sink is ever added, per-sink success tracking must
/// land too — re-sending a batch that one sink already accepted would
/// double-insert into that sink (the `events` INSERT is not idempotent).
async fn flush(
    sinks: &[Arc<dyn EventSink>],
    buf: &mut Vec<OwnedEvent>,
    cfg: &BufferedWriterConfig,
) -> bool {
    if buf.is_empty() {
        return true;
    }
    let count = buf.len();

    // Reborrow each owned view (yielding a `&OpenLineageEventView`) into a row.
    // An empty (`event = None`) view yields no row and is skipped.
    let rows: Vec<_> = buf
        .iter()
        .filter_map(|ev| event_to_row(ev.reborrow()))
        .collect();

    // Nothing to persist (all views empty) — clearing is correct, no data lost.
    if rows.is_empty() {
        buf.clear();
        return true;
    }

    let mut backoff = cfg.flush_retry_backoff;
    for attempt in 0..=cfg.flush_max_retries {
        if append_all(sinks, &rows).await {
            buf.clear();
            return true;
        }
        // Don't sleep after the final attempt.
        if attempt < cfg.flush_max_retries {
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2);
        }
    }

    // Every retry failed. Keep the events buffered for the next trigger rather
    // than dropping them. Warn loudly when the retained buffer grows past the
    // channel depth — the sink has been down long enough that ingest is now
    // backpressured (the bounded channel is filling behind this retained batch).
    if buf.len() >= cfg.channel_capacity {
        tracing::error!(
            buffered = buf.len(),
            "sink down: {count} events retained for retry, ingest is backpressured"
        );
    } else {
        tracing::warn!("flush failed, retaining {count} events for retry");
    }
    false
}

/// Append `rows` to every sink. Returns `true` only if *all* sinks succeeded;
/// a failing sink is logged and the next sink still runs.
async fn append_all(sinks: &[Arc<dyn EventSink>], rows: &[crate::writer::row::EventRow]) -> bool {
    let mut all_ok = true;
    for sink in sinks {
        if let Err(e) = sink.append(rows).await {
            tracing::error!("{} flush failed ({} events): {e}", sink.name(), rows.len());
            all_ok = false;
        }
    }
    all_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::convert_event;
    use crate::writer::row::EventRow;
    use crate::writer::sink::SinkError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// A sink that records how many rows and how many flush calls it has seen.
    struct CountingSink {
        rows: AtomicUsize,
        flushes: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl EventSink for CountingSink {
        fn name(&self) -> &'static str {
            "counting"
        }
        async fn append(&self, rows: &[EventRow]) -> Result<(), SinkError> {
            if rows.is_empty() {
                return Ok(());
            }
            self.rows.fetch_add(rows.len(), Ordering::SeqCst);
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

    fn counting() -> (Arc<CountingSink>, Vec<Arc<dyn EventSink>>) {
        let sink = Arc::new(CountingSink {
            rows: AtomicUsize::new(0),
            flushes: AtomicUsize::new(0),
        });
        let sinks: Vec<Arc<dyn EventSink>> = vec![sink.clone()];
        (sink, sinks)
    }

    /// A sink that returns `Err` for its first `fail_first` `append` calls (with
    /// non-empty rows), then succeeds — modelling a transient outage. Records the
    /// total rows it has accepted (successful calls only) and every call attempt.
    struct FailingSink {
        fail_first: usize,
        calls: AtomicUsize,
        accepted_rows: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl EventSink for FailingSink {
        fn name(&self) -> &'static str {
            "failing"
        }
        async fn append(&self, rows: &[EventRow]) -> Result<(), SinkError> {
            if rows.is_empty() {
                return Ok(());
            }
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                return Err(SinkError::Postgres("transient".into()));
            }
            self.accepted_rows.fetch_add(rows.len(), Ordering::SeqCst);
            Ok(())
        }
    }

    /// A `BufferedWriterConfig` with fast retries for tests, overriding only the
    /// fields a test cares about via the closure.
    fn test_cfg(f: impl FnOnce(&mut BufferedWriterConfig)) -> BufferedWriterConfig {
        let mut cfg = BufferedWriterConfig {
            buffer_size: 100,
            flush_interval: Duration::from_secs(3600),
            channel_capacity: 16,
            flush_max_retries: 3,
            flush_retry_backoff: Duration::from_millis(1),
        };
        f(&mut cfg);
        cfg
    }

    /// A generous drain timeout for shutdown in tests that expect a clean drain.
    const TEST_DRAIN: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn flushes_when_buffer_size_reached() {
        let (sink, sinks) = counting();
        // Large interval so only the size trigger can fire within the test.
        let writer = BufferedWriter::spawn(sinks, test_cfg(|c| c.buffer_size = 3));
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
        writer.shutdown(TEST_DRAIN).await;
    }

    #[tokio::test]
    async fn flushes_on_interval_below_buffer_size() {
        let (sink, sinks) = counting();
        let writer = BufferedWriter::spawn(
            sinks,
            test_cfg(|c| c.flush_interval = Duration::from_millis(50)),
        );
        let h = writer.handle();
        h.enqueue(event("r0")).await.unwrap();
        h.enqueue(event("r1")).await.unwrap();
        // Wait past the interval; the time trigger should flush the 2 events.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(sink.flushes.load(Ordering::SeqCst) >= 1);
        assert_eq!(sink.rows.load(Ordering::SeqCst), 2);
        drop(h);
        writer.shutdown(TEST_DRAIN).await;
    }

    #[tokio::test]
    async fn drains_remaining_on_shutdown() {
        let (sink, sinks) = counting();
        let writer = BufferedWriter::spawn(sinks, test_cfg(|_| {}));
        let h = writer.handle();
        h.enqueue(event("r0")).await.unwrap();
        h.enqueue(event("r1")).await.unwrap();
        drop(h);
        // shutdown drops the last sender, closing the channel and forcing a
        // final drain flush.
        writer.shutdown(TEST_DRAIN).await;
        assert_eq!(sink.rows.load(Ordering::SeqCst), 2);
    }

    /// A transient sink failure must NOT drop events: the interval flush fails,
    /// the events stay buffered, and the shutdown drain (after the sink recovers)
    /// writes all of them.
    #[tokio::test]
    async fn transient_failure_then_success_writes_all() {
        let sink = Arc::new(FailingSink {
            fail_first: 2,
            calls: AtomicUsize::new(0),
            accepted_rows: AtomicUsize::new(0),
        });
        let sinks: Vec<Arc<dyn EventSink>> = vec![sink.clone()];
        // Short interval so a flush fires (and fails) before shutdown; retries
        // are fast. fail_first=2 with 3 max-retries means the first flush burns
        // attempts then succeeds on the 3rd within a single flush call.
        let writer = BufferedWriter::spawn(
            sinks,
            test_cfg(|c| {
                c.flush_interval = Duration::from_millis(20);
                c.flush_max_retries = 5;
            }),
        );
        let h = writer.handle();
        h.enqueue(event("r0")).await.unwrap();
        h.enqueue(event("r1")).await.unwrap();
        drop(h);
        writer.shutdown(TEST_DRAIN).await;
        // No events lost despite the first two append attempts failing.
        assert_eq!(sink.accepted_rows.load(Ordering::SeqCst), 2);
    }

    /// While a sink is down, the buffer must be RETAINED (not cleared) so the
    /// events are still there to write once it recovers. We assert the writer
    /// eventually persists everything across a failure window.
    #[tokio::test]
    async fn sink_failure_retains_events_until_recovery() {
        let sink = Arc::new(FailingSink {
            // Fail enough times that the first flush's retries are all exhausted
            // (max_retries=2 → 3 tries), forcing a re-buffer, then recover.
            fail_first: 3,
            calls: AtomicUsize::new(0),
            accepted_rows: AtomicUsize::new(0),
        });
        let sinks: Vec<Arc<dyn EventSink>> = vec![sink.clone()];
        let writer = BufferedWriter::spawn(
            sinks,
            test_cfg(|c| {
                c.flush_interval = Duration::from_millis(20);
                c.flush_max_retries = 2;
            }),
        );
        let h = writer.handle();
        h.enqueue(event("r0")).await.unwrap();
        h.enqueue(event("r1")).await.unwrap();
        // Let the first interval flush exhaust its retries and re-buffer.
        tokio::time::sleep(Duration::from_millis(80)).await;
        drop(h);
        // The drain keeps retrying until the sink recovers (after 3 failed
        // calls); all events land, none dropped.
        writer.shutdown(TEST_DRAIN).await;
        assert_eq!(sink.accepted_rows.load(Ordering::SeqCst), 2);
    }
}
