//! Asynchronous projection worker.
//!
//! The ingest path only appends raw events to `events` (the source of truth).
//! This worker tails that log in `(xid, seq)` order, below the visibility
//! watermark (see `project_once`), and folds each event into the normalized
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
                // Another projector holding the cursor reads as "nothing to do":
                // it is already folding these events, so don't queue behind it.
                loop {
                    match project_once(&pool, &registry, OnContention::Skip).await {
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
                    while let Ok(n) = project_once(&pool, &registry, OnContention::Skip).await {
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

/// What [`project_once`] does when another projector holds the cursor.
#[derive(Clone, Copy)]
enum OnContention {
    /// Block until the other projector commits, then fold from its cursor.
    /// For callers whose contract is "caught up on return" ([`project_all`],
    /// [`rebuild`]).
    Wait,
    /// Return 0 right away: the holder is already folding these events. For
    /// the background loop, so idle replicas don't each pin a pooled
    /// connection queueing on the lock.
    Skip,
}

/// Fold one batch of up to [`BATCH`] events after the cursor into the read
/// tables, in a single transaction, advancing the cursor. Returns the number of
/// events applied (0 when the log has caught up, or when `on_contention` is
/// [`Skip`](OnContention::Skip) and another projector holds the cursor).
///
/// The cursor row is locked for the whole transaction, so concurrent projectors
/// (one per replica or Neon isolate) fold disjoint batches instead of each
/// re-folding the same one, and a slower projector can't move the cursor back.
///
/// Only rows below the snapshot's xmin are read, in `(xid, seq)` order, and the
/// cursor is that pair. `seq` is assigned at insert, not at commit, so a
/// `seq` cursor could pass a row whose writer hadn't committed yet and skip it
/// for good (ADR 0016). Every transaction below xmin has finished, so nothing
/// new can appear behind the cursor.
async fn project_once(
    pool: &PgPool,
    registry: &ProcessorRegistry,
    on_contention: OnContention,
) -> Result<usize, sqlx::Error> {
    let started = std::time::Instant::now();
    let applier = PgApplier;
    let mut tx = pool.begin().await?;

    let lock_sql = match on_contention {
        OnContention::Wait => {
            "SELECT last_xid::text::bigint, last_seq FROM projection_state \
             WHERE name = $1 FOR UPDATE"
        }
        OnContention::Skip => {
            "SELECT last_xid::text::bigint, last_seq FROM projection_state \
             WHERE name = $1 FOR UPDATE SKIP LOCKED"
        }
    };
    let Some((last_xid, last_seq)): Option<(i64, i64)> = sqlx::query_as(lock_sql)
        .bind(CURSOR)
        .fetch_optional(&mut *tx)
        .await?
    else {
        // Only `SKIP LOCKED` yields no row: the cursor row always exists
        // (seeded by the initial migration).
        return Ok(0);
    };

    let rows = sqlx::query_as::<_, LogRow>(
        "SELECT xid::text::bigint AS xid, seq, event_kind, event_type, event_time, run_id, \
                job_namespace, job_name, dataset_namespace, dataset_name, \
                raw, inputs, outputs, column_lineage \
         FROM events \
         WHERE (xid, seq) > ($1::bigint::text::xid8, $2) \
           AND xid < pg_snapshot_xmin(pg_current_snapshot()) \
         ORDER BY xid, seq LIMIT $3",
    )
    .bind(last_xid)
    .bind(last_seq)
    .bind(BATCH)
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        return Ok(0);
    }

    let mut cursor = (last_xid, last_seq);
    let n = rows.len();
    for LogRow { xid, ev } in rows {
        // Rows arrive in `(xid, seq)` order, so the last one is the new cursor.
        cursor = (xid, ev.seq);
        // Parse (backend-agnostic) then apply (backend-specific) — the seam.
        for m in registry.process(&ev) {
            applier.apply(&mut tx, &m).await?;
        }
    }

    sqlx::query(
        "UPDATE projection_state SET last_xid = $1::bigint::text::xid8, last_seq = $2 \
         WHERE name = $3",
    )
    .bind(cursor.0)
    .bind(cursor.1)
    .bind(CURSOR)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    tracing::debug!(
        events = n,
        cursor_xid = cursor.0,
        cursor_seq = cursor.1,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "projected batch"
    );
    Ok(n)
}

/// Synchronously fold every event currently after the cursor into the read
/// tables, returning the total number applied. Events from writers still in
/// flight wait for a later call (they are above the watermark). Drives the same per-batch fold
/// the background task uses; handy for tests and one-shot replays where waiting
/// on the poll interval is undesirable.
pub async fn project_all(pool: &PgPool) -> Result<usize, sqlx::Error> {
    let registry = ProcessorRegistry::with_well_known();
    let mut total = 0;
    loop {
        let n = project_once(pool, &registry, OnContention::Wait).await?;
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
    sqlx::query("UPDATE projection_state SET last_xid = '0', last_seq = 0 WHERE name = $1")
        .bind(CURSOR)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    while project_once(pool, &registry, OnContention::Wait).await? > 0 {}
    Ok(())
}

/// A [`RawEvent`] plus the xid of the transaction that inserted it, the first
/// half of the projection cursor. Kept out of `RawEvent`, since processors
/// have no use for it.
#[derive(sqlx::FromRow)]
struct LogRow {
    xid: i64,
    #[sqlx(flatten)]
    ev: RawEvent,
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

#[cfg(all(test, feature = "postgres-it"))]
mod cursor_lock_tests {
    //! Concurrent projectors must fold disjoint batches. Needs Docker; gated
    //! behind `postgres-it`. Run with:
    //!   cargo test -p headwaters --features postgres-it cursor_lock
    use super::*;
    use crate::ingest::convert_event;
    use crate::test_support::start_postgres;
    use crate::writer::postgres::PostgresSink;
    use crate::writer::row::event_to_row;
    use crate::writer::sink::EventSink;

    /// Append `n` distinct run events to the log without projecting them.
    async fn append_events(pool: &PgPool, n: usize) {
        let sink = PostgresSink::new(pool.clone());
        for i in 0..n {
            let json = format!(
                r#"{{"eventType":"COMPLETE","eventTime":"2023-11-14T22:13:20Z",
                    "producer":"test-producer","run":{{"runId":"run-{i}"}},
                    "job":{{"namespace":"etl","name":"job_{i}"}}}}"#
            );
            let event = convert_event(json.as_bytes()).expect("convert event");
            let row = event_to_row(event.reborrow()).expect("event row");
            sink.append(&[row]).await.expect("append");
        }
    }

    async fn cursor(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT last_seq FROM projection_state WHERE name = $1")
            .bind(CURSOR)
            .fetch_one(pool)
            .await
            .expect("read cursor")
    }

    /// Hold the cursor row lock the way a projector mid-batch does.
    async fn hold_cursor(pool: &PgPool) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut tx = pool.begin().await.expect("begin");
        sqlx::query("SELECT 1 FROM projection_state WHERE name = $1 FOR UPDATE")
            .bind(CURSOR)
            .execute(&mut *tx)
            .await
            .expect("lock cursor");
        tx
    }

    /// Insert run event `i` (job `job_{i}`) through `conn`, with an explicit
    /// `seq` when given. Lets a test hold a writer's transaction open, or
    /// order `seq` against xid by hand.
    async fn insert_event(conn: &mut sqlx::PgConnection, i: usize, seq: Option<i64>) {
        sqlx::query(
            "INSERT INTO events (seq, event_kind, event_type, event_time, run_id, \
                                 job_namespace, job_name) \
             VALUES (COALESCE($1, nextval('events_seq_seq')), 'run', 'COMPLETE', \
                     '2023-11-14T22:13:20Z', $2, 'etl', $3)",
        )
        .bind(seq)
        .bind(format!("run-{i}"))
        .bind(format!("job_{i}"))
        .execute(conn)
        .await
        .expect("insert event");
    }

    async fn job_names(pool: &PgPool) -> Vec<String> {
        sqlx::query_scalar("SELECT name FROM jobs ORDER BY name")
            .fetch_all(pool)
            .await
            .unwrap()
    }

    /// #69: a writer that takes a lower `seq` but commits after a higher one
    /// must still have its event folded.
    #[tokio::test]
    async fn late_committing_writer_is_not_skipped() {
        let db = start_postgres().await;

        let mut slow = db.pool.begin().await.unwrap();
        insert_event(&mut slow, 0, None).await; // seq 1, uncommitted
        let mut fast = db.pool.acquire().await.unwrap();
        insert_event(&mut fast, 1, None).await; // seq 2, committed
        drop(fast);

        project_all(&db.pool).await.expect("project");
        slow.commit().await.unwrap();
        project_all(&db.pool).await.expect("project");

        assert_eq!(job_names(&db.pool).await, ["job_0", "job_1"]);
    }

    /// `seq` and xid order can disagree: `nextval` runs before the inserting
    /// transaction takes its xid. A `seq` cursor would pass the lower-`seq`
    /// row even below the watermark; the `(xid, seq)` cursor must not.
    #[tokio::test]
    async fn lower_seq_with_higher_xid_is_not_skipped() {
        let db = start_postgres().await;
        let reserved: i64 = sqlx::query_scalar("SELECT nextval('events_seq_seq')")
            .fetch_one(&db.pool)
            .await
            .unwrap();

        // Higher seq, lower xid, committed first.
        let mut early = db.pool.acquire().await.unwrap();
        insert_event(&mut early, 0, None).await;
        drop(early);
        // Lower (reserved) seq, higher xid, still in flight.
        let mut late = db.pool.begin().await.unwrap();
        insert_event(&mut late, 1, Some(reserved)).await;

        project_all(&db.pool).await.expect("project");
        late.commit().await.unwrap();
        project_all(&db.pool).await.expect("project");

        assert_eq!(job_names(&db.pool).await, ["job_0", "job_1"]);
    }

    #[tokio::test]
    async fn concurrent_projectors_fold_each_event_once() {
        let db = start_postgres().await;
        append_events(&db.pool, 20).await;

        // Before the lock, each of these read the same cursor and re-folded
        // the same batch, so the applied counts summed past 20.
        let projectors: Vec<_> = (0..4)
            .map(|_| {
                let pool = db.pool.clone();
                tokio::spawn(async move { project_all(&pool).await })
            })
            .collect();
        let mut total = 0;
        for p in projectors {
            total += p.await.unwrap().expect("project");
        }

        assert_eq!(total, 20, "every event must be folded exactly once");
        let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(jobs, 20);
    }

    #[tokio::test]
    async fn skip_returns_immediately_while_another_projector_holds_the_cursor() {
        let db = start_postgres().await;
        append_events(&db.pool, 3).await;
        let registry = ProcessorRegistry::with_well_known();

        let held = hold_cursor(&db.pool).await;
        let n = tokio::time::timeout(
            Duration::from_secs(5),
            project_once(&db.pool, &registry, OnContention::Skip),
        )
        .await
        .expect("Skip must not block on the lock")
        .expect("project");
        assert_eq!(n, 0, "a held cursor reads as nothing to do");
        assert_eq!(cursor(&db.pool).await, 0);

        held.rollback().await.unwrap();
        let n = project_once(&db.pool, &registry, OnContention::Skip)
            .await
            .expect("project");
        assert_eq!(n, 3, "once released, the next tick folds the backlog");
    }

    #[tokio::test]
    async fn wait_blocks_until_the_holder_commits_then_catches_up() {
        let db = start_postgres().await;
        append_events(&db.pool, 3).await;

        let held = hold_cursor(&db.pool).await;
        let pool = db.pool.clone();
        let waiting = tokio::spawn(async move { project_all(&pool).await });

        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!waiting.is_finished(), "Wait must queue behind the holder");

        held.commit().await.unwrap();
        let n = waiting.await.unwrap().expect("project");
        assert_eq!(n, 3);
        assert_eq!(cursor(&db.pool).await, 3);
    }
}
