## proto

- convert RunDetail.state to enum.
- Dataset: allow for different table types (other than DB_TABLE)

## Datafusion

- DataFusion executes CTAS writes without the instrumented planner seeing the output

## Projection / writer — deferred data-loss fixes

Surfaced by the 2026-06-29 crate review (fixes #1–6, #9 landed; the projection
meta-upsert item was since resolved for datasets — the run side was investigated
and is not reachable, since `RunMetaProcessor` and `RunStateProcessor` gate on
the same run+job identity, so the `runs` row always exists when run meta folds).

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
