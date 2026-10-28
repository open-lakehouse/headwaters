# 0016 — Projection liveness and topology on Neon Functions

> Status: **Accepted** (2026-10). Builds on
> [0006](0006-hybrid-cqrs-postgres-storage.md) (async projection) and
> [0015](0015-neon-functions-deployment-via-napi.md) (Neon Functions). Fixes
> [#69](https://github.com/open-lakehouse/headwaters/issues/69). Neon facts
> below were checked against the docs on 2026-10-28.

## Context

ADR 0015 runs one buffered writer and one projector in every Neon Function
isolate. Two problems came up while preparing the first deploy:

1. **Skipped events (#69).** `events.seq` is a `BIGSERIAL`, so a row gets its
   `seq` when it is inserted, not when it commits. With several writers, a
   lower `seq` can become visible after a higher one. The projector folded
   `seq > last_seq` and then moved the cursor to the highest `seq` it saw, so a
   row that committed late fell behind the cursor and was never folded.
2. **Liveness.** The projector is a background tokio task. The Neon runtime
   promises nothing about background timers between requests. It only keeps
   request handling and `waitUntil` work alive. Isolates shut down when idle
   (SIGINT, killed 5 s later), and Neon may evict them for operational reasons.
   Events ingested just before an eviction are folded only in the shutdown
   drain, whose 3 s budget the writer drain also uses, or on the next cold start.

We also asked whether the projector should become its own Neon Function to
separate background work from the API and to scale it independently, and
whether Neon's event machinery could drive it.

What Neon offers (sources at the end):

- **Function triggers:** `schedule` (cron in UTC, at most once a minute) and
  `storage_object_created`. A trigger POSTs a JSON envelope to a function slug
  and path. The `invocation_id` is "stable across retries", so the handler must
  be idempotent. There is no trigger for database changes.
- **`waitUntil`** (from `@neon/functions`) keeps a promise running after the
  response for up to 15 min. An isolate with pending `waitUntil` work is not
  idle.
- **Realtime** (public beta since 2026-10-15) keeps live *query results* in
  sync. The incremental work runs on a replica, and there is a server SDK.
  There is no durable resume cursor, "recovery may deliver a full replacement
  result", there is no RLS yet, and the protocol is still changing.
- Each account can run 100 concurrent invocations. `DATABASE_URL` goes through
  PgBouncer in transaction mode.

## Decision

### 1. Project only below the visibility watermark, with a `(xid, seq)` cursor

Every `events` row records the transaction that inserted it
(`xid xid8 DEFAULT pg_current_xact_id()`). The projector folds only rows whose
transaction is below the **xmin** of its own snapshot
(`pg_snapshot_xmin(pg_current_snapshot())`). Every transaction below xmin has
already committed or aborted, so no new row can appear in that range. This is
the usual outbox and log-tailing technique.

The cursor is the pair `(last_xid, last_seq)`, and rows are read in
`(xid, seq)` order. Keeping a `seq` cursor and adding the xmin filter would
still lose rows. Transaction ids and `seq` values are handed out at different
moments, so transaction A can hold the lower xid and the *higher* seq. When the
lower-seq transaction commits last, a `max(seq)` cursor has already passed it.
Ordering by `(xid, seq)` is stable below xmin, so a cursor in that order never
skips a row.

Rows from before this migration have the frozen xid `0`. They sort first and
keep their `seq` order, and the cursor migrates as `(0, last_seq)`.

### 2. Make projection a bounded unit of work that a host can invoke

`postgres.projection_mode` selects how projection is hosted:

- `background` (default). The polling loop, as before. Right for the
  standalone binary and for long-lived replicas.
- `on_demand`. No loop. Projection runs when something asks for it, through
  one operation, **settle**: wait for the buffered writer to flush, then fold
  for at most a time budget. It reports how many events it applied and
  whether it caught up.
  - **Embedding hosts** call it directly (`embed::Service::settler()`).
  - **Trigger route:** `POST /internal/projection/run` settles for external
    schedulers. It exists only when a shared secret is configured, and every
    request must send `Authorization: Bearer <secret>`.

On Neon, the function runs in `on_demand` mode:

- After each accepted ingest, the handler starts a settle and registers it
  with `waitUntil`, so the isolate is not idle until the fold finishes.
  Concurrent ingests share one settle.
- A **cron trigger** POSTs to the same path as a safety net, so events are
  folded even after an isolate was evicted. Its cadence is an operational
  choice, not a constant (see below). Trigger deliveries
  can't carry a custom secret. The handler instead accepts them by the
  `X-Neon-Trigger-Invocation-Id` header, which Neon strips from client
  requests, and settles through the addon directly.

Cadence and freshness are configured per deployment, because the right
trade-off depends on how fresh reads must be and what idle compute may cost:

- **The cron schedule** is a `just neon-trigger` parameter. It defaults to
  every 10 minutes. Neon allows at most one run a minute.
- **`HEADWATERS_NEON_SETTLE_ON_INGEST`** (default `true`). When `false`,
  ingest only waits for the writer flush (still under `waitUntil`, so buffered
  events don't depend on the SIGINT drain), and the cron is the only fold.
  Reads then lag by up to one interval.
- **`HEADWATERS_NEON_TRIGGER_BUDGET_MS`** (default 50 s, at most 14 min). A
  trigger answers only once its settle is done, and Neon cuts off handlers
  that don't respond within 15 min.

The default cadence is 10 minutes, not 1, because of Neon's scale-to-zero:
the branch compute suspends after 5 minutes without activity, and every
trigger run queries it. A cadence under that window keeps the compute awake
around the clock, even with no traffic. With settle-on-ingest on, the cron
only catches events left by evicted isolates, so the slower cadence costs
little freshness.

Long-lived hosts that use the Bearer-guarded route choose their own cadence
and pass `?budget_ms=`.

### 3. One function for now; a dedicated projector function is a deploy choice

The trigger route is part of the same artifact. A dedicated
`headwaters-projector` function is the same bundle deployed with a different
environment and the cron trigger pointed at it. We are not splitting yet.

## Alternatives considered

- **Move the projector into its own function now.** This does not fix #69,
  because the race is between the writers and the projector, and one projector
  still misses rows when many isolates write. It also adds no fold throughput.
  The cursor row lock (#67) already serializes folding, so one projector folds
  exactly as fast as N isolates taking turns. What it does buy is operational:
  no per-isolate polling, and API and fold load kept apart. Those benefits
  come from step 2 on its own. Splitting becomes worthwhile once metrics show
  fold work disturbing API latency.
- **Drive projection from Realtime.** A projector needs a durable offset and a
  complete, ordered stream of changes. Realtime has no resume cursor, may
  replace the whole result after recovery, runs on a lagging replica, and is
  still in beta. It also needs a long-lived subscriber, which Functions don't
  host well. `events` is already the durable log, so we tail it directly.
  Realtime would fit the *read* side, for example live lineage graphs and run
  status in the UI, and that is left for a future decision.
- **`LISTEN`/`NOTIFY` as the wake-up signal.** It needs a session that stays
  connected, which transaction-mode PgBouncer can't provide, and like Realtime
  it needs a long-lived listener. Settling after ingest does the same job and
  needs neither.
- **Serialize appends with `pg_advisory_xact_lock`.** Simple, but it caps
  ingest at one writer at a time, which is the opposite of what scaling out on
  Neon is for.
- **Detect gaps in `seq`.** Sequences legitimately have gaps from rolled-back
  inserts and from `CACHE`, so a gap that will never fill looks the same as one
  that is about to.

## Consequences

- With multiple writers, projection is correct: every committed event is folded
  exactly once (#69). `projection::rebuild` stays the recovery tool for logic
  changes, not for lost rows.
- **A long-running transaction anywhere in the database holds xmin back and
  stalls projection** until it ends. Reads go stale but are never wrong.
  `rebuild`, migrations and ad-hoc analytics sessions should stay short, or run
  where they don't overlap ingest.
- In `on_demand` mode on Neon with settle-on-ingest, events are usually
  folded right after their flush. In the worst case, when the isolate is
  evicted mid-fold, they wait up to one cron interval (10 minutes by default).
  With settle-on-ingest off, every event waits for the next cron run. A long-lived host in `on_demand`
  mode folds only when settled, so it needs a scheduler on the trigger route.
- **Scale-out path, not taken now:** the fold is idempotent and doesn't depend
  on order (0006), so if fold throughput is ever measured to be the bottleneck,
  the single cursor can be split into per-key partitions that projectors claim
  with `FOR UPDATE SKIP LOCKED`, giving truly parallel projectors.

## Sources

- Neon Functions: neon.com/docs/compute/functions/{overview,
  triggers/overview, triggers/schedule, reference/runtime-limits}
- Neon scale to zero: neon.com/docs/introduction/scale-to-zero (default
  5 minute suspend; fixed on the Free plan)
- Neon Realtime: neon.com/blog/neon-now-has-realtime,
  neon.com/docs/realtime/{guides/subscribing, sdks/typescript}
- PostgreSQL: `pg_current_xact_id`, `pg_current_snapshot`, `pg_snapshot_xmin`
  (System Information Functions, "Transaction ID and Snapshot Information").
