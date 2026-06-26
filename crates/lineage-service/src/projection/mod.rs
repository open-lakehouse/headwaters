//! Asynchronous projection worker.
//!
//! The ingest path only appends raw events to `events` (the source of truth).
//! This worker tails that log by `seq` and folds each event into the normalized
//! read tables (`namespaces`, `jobs`, `runs`, `datasets`, `lineage_edges`) that
//! the Marquez-compatible read API queries. Reads therefore lag ingestion by at
//! most one poll interval — eventual consistency, which a lineage browse UI
//! tolerates — in exchange for an ingest path that never blocks on
//! normalization.
//!
//! Everything here is idempotent: each event's effect is an upsert guarded by
//! the event time (latest-event-wins for edges/metadata/schema; terminal run
//! states never downgraded), so replaying the whole log — or re-applying an
//! event after a crash mid-batch — reproduces the same read tables. That is
//! what makes [`rebuild`] (truncate + reset cursor + re-fold) safe.

// The projection extension surface: the `FacetProcessor` trait (custom
// processors are passed to `Projector::spawn_with`), the backend-agnostic
// `Mutation` IR they emit, and the `MutationApplier` seam a new storage backend
// implements. `processors` holds the concrete built-in impls and is an internal
// detail of `registry::with_well_known`.
pub mod applier;
pub mod backend;
pub mod mutation;
pub mod processor;
pub(crate) mod processors;
pub mod registry;

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use tokio::task::JoinHandle;

use backend::postgres::PgApplier;
use registry::ProcessorRegistry;

/// The projector's cursor name in `projection_state`.
const CURSOR: &str = "marquez";

/// How many events to fold per poll iteration before committing the cursor.
const BATCH: i64 = 500;

/// Owns the background projection task.
pub struct Projector {
    task: JoinHandle<()>,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl Projector {
    /// Spawn the projection task with the built-in processors, polling every
    /// `interval`.
    pub fn spawn(pool: PgPool, interval: Duration) -> Self {
        Self::spawn_with(pool, interval, Vec::new())
    }

    /// Spawn the projection task with the built-in processors plus `extra`
    /// custom processors (appended after the built-ins).
    pub fn spawn_with(
        pool: PgPool,
        interval: Duration,
        extra: Vec<Box<dyn processor::FacetProcessor>>,
    ) -> Self {
        let mut registry = ProcessorRegistry::with_well_known();
        for p in extra {
            registry.register(p);
        }
        let (shutdown, rx) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(run(pool, interval, Arc::new(registry), rx));
        Self { task, shutdown }
    }

    /// Signal the task to stop and await a final drain.
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

async fn run(
    pool: PgPool,
    interval: Duration,
    registry: Arc<ProcessorRegistry>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                // Drain everything currently available, then go back to sleep.
                loop {
                    match project_once(&pool, &registry).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(e) => {
                            tracing::error!("projection batch failed: {e}");
                            break;
                        }
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    // Final drain before exit.
                    while let Ok(n) = project_once(&pool, &registry).await {
                        if n == 0 {
                            break;
                        }
                    }
                    return;
                }
            }
        }
    }
}

/// Fold one batch of up to [`BATCH`] events after the cursor into the read
/// tables, in a single transaction, advancing the cursor. Returns the number of
/// events applied (0 when the log has caught up).
async fn project_once(pool: &PgPool, registry: &ProcessorRegistry) -> Result<usize, sqlx::Error> {
    let started = std::time::Instant::now();
    let applier = PgApplier;
    let mut tx = pool.begin().await?;

    let last_seq: i64 = sqlx::query_scalar("SELECT last_seq FROM projection_state WHERE name = $1")
        .bind(CURSOR)
        .fetch_one(&mut *tx)
        .await?;

    let rows = sqlx::query_as::<_, RawEvent>(
        "SELECT seq, event_kind, event_type, event_time, run_id, \
                job_namespace, job_name, dataset_namespace, dataset_name, \
                raw, inputs, outputs, column_lineage \
         FROM events WHERE seq > $1 ORDER BY seq ASC LIMIT $2",
    )
    .bind(last_seq)
    .bind(BATCH)
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let mut max_seq = last_seq;
    let n = rows.len();
    for ev in rows {
        max_seq = max_seq.max(ev.seq);
        // Parse (backend-agnostic) then apply (backend-specific) — the seam.
        for m in registry.process(&ev) {
            applier.apply(&mut tx, &m).await?;
        }
    }

    sqlx::query("UPDATE projection_state SET last_seq = $1 WHERE name = $2")
        .bind(max_seq)
        .bind(CURSOR)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    tracing::debug!(
        events = n,
        cursor = max_seq,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "projected batch"
    );
    Ok(n)
}

/// Synchronously fold every event currently after the cursor into the read
/// tables, returning the total number applied. Drives the same per-batch fold
/// the background task uses; handy for tests and one-shot replays where waiting
/// on the poll interval is undesirable.
pub async fn project_all(pool: &PgPool) -> Result<usize, sqlx::Error> {
    let registry = ProcessorRegistry::with_well_known();
    let mut total = 0;
    loop {
        let n = project_once(pool, &registry).await?;
        if n == 0 {
            return Ok(total);
        }
        total += n;
    }
}

/// Truncate the read tables and reset the cursor, then re-fold the entire event
/// log. Used to rebuild the projection after a schema or logic change.
pub async fn rebuild(pool: &PgPool) -> Result<(), sqlx::Error> {
    let registry = ProcessorRegistry::with_well_known();
    let mut tx = pool.begin().await?;
    sqlx::query(
        "TRUNCATE namespaces, jobs, runs, datasets, lineage_edges, \
                  dataset_fields, column_lineage_edges, dataset_versions, \
                  sources, tags, tag_assignments RESTART IDENTITY CASCADE",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE projection_state SET last_seq = 0 WHERE name = $1")
        .bind(CURSOR)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    while project_once(pool, &registry).await? > 0 {}
    Ok(())
}

/// One row of the projection's source query — the promoted columns plus the
/// JSON blobs the processors read. Public because it is the input type of the
/// [`FacetProcessor`](processor::FacetProcessor) extension point.
#[derive(sqlx::FromRow)]
pub struct RawEvent {
    pub seq: i64,
    pub event_kind: String,
    pub event_type: Option<String>,
    pub event_time: Option<chrono::DateTime<chrono::Utc>>,
    pub run_id: Option<String>,
    pub job_namespace: Option<String>,
    pub job_name: Option<String>,
    pub dataset_namespace: Option<String>,
    pub dataset_name: Option<String>,
    pub raw: Option<serde_json::Value>,
    pub inputs: Option<serde_json::Value>,
    pub outputs: Option<serde_json::Value>,
    /// The writer-lifted per-event column-lineage document
    /// (`{inputs:[...], outputs:[...]}`); consumed by the column-lineage
    /// processor.
    pub column_lineage: Option<serde_json::Value>,
}
