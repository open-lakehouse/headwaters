## proto

- convert RunDetail.state to enum.
- Dataset: allow for different table types (other than DB_TABLE)

## Datafusion

- DataFusion executes CTAS writes without the instrumented planner seeing the output

## Projection / writer — deferred data-loss fixes

Surfaced by the 2026-06-29 crate review (fixes #1–6, #9 landed; these two were
deferred because the correct fix is architectural and needs a design decision).

- **Buffered writer drops events on sink failure.**
  `crates/headwaters/src/writer/buffered.rs` — `flush()` calls `buf.clear()`
  unconditionally, even when every sink's `append` returned `Err`. A transient
  Postgres outage silently drops the whole flush (only logged), contradicting the
  module's "backpressure, not drop" promise for the terminal source-of-truth
  writer. The shutdown drain path has the same flaw, so a restart during a DB blip
  can lose the tail of accepted (HTTP 202) events.
  *Fix direction:* retain-on-failure with bounded retry, or surface the failure as
  backpressure (stop draining so `enqueue` blocks the client). Decide what an
  accepted-but-undelivered event means for the 202 contract; add a sink-failure
  test asserting events are not dropped.

- **Projection meta UPDATEs no-op when the target row is absent.**
  `crates/headwaters/src/projection/backend/postgres.rs` — `set_run_meta` and
  `set_dataset_meta` are bare `UPDATE … WHERE` with no insert. A run-facets event
  with a `run_id` but no job block (only `RunStateProcessor` creates the `runs`
  row, and it requires job ns+name) loses its nominal/parent/error facets; a
  lifecycle-DROP arriving before any schema/edge event for a dataset loses the
  deletion (the `datasets` row doesn't exist yet).
  *Fix direction:* convert to upserts (`INSERT … ON CONFLICT DO UPDATE` mirroring
  the existing latest-wins `meta_at` guard) so meta survives regardless of
  processor ordering. Keep replay-safety (`rebuild` re-folds the whole log) and the
  `meta_at` GREATEST guard intact; add postgres-it coverage for both orderings.
