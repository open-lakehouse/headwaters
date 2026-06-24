# 0003 — Per-statement `run_id` for START/COMPLETE correlation

> Status: **Accepted** (2026-06). Implemented in `crates/hydrofoil/src/engine.rs`
> (`StoredStatement`) and `crates/hydrofoil/src/server.rs`. Refines
> [`docs/open-lineage-design.md`](../open-lineage-design.md) and
> [`docs/session-management.md`](../session-management.md).

## Context

A query's OpenLineage events fire from two different RPCs:

```text
get_flight_info_statement   → create_physical_plan   → START
do_get_statement(ticket)    → execution stream end   → COMPLETE / FAIL
```

`LineageContext.run_id` is `Option<Uuid>`; when `None`, the planner mints a fresh
`Uuid::now_v7()` at plan time. Because the two RPCs built independent sessions and
neither pinned a run id, START and COMPLETE were minted **separately** and never
shared a `runId` — so events for one query could not be joined downstream.

## Decision

Pin a run id at statement creation and carry it through the session-scoped
statement store; mint a **fresh** run id per execution, parented to the pinned
one:

- In `get_flight_info_statement` / `do_action_create_prepared_statement`: mint
  `run_id = Uuid::now_v7()`, build the `LineageContext` from the request metadata
  with `run_id` set, and store it in the planning session's statement store as
  `StoredStatement { plan, lineage, created_at }`. This is the statement's
  *planning/creation* run id.
- In `do_get_statement` / `do_get_prepared_statement`: look the statement up in
  the resolved session and, via `crate::lineage::execution_context`, derive a
  per-execution context that **mints a fresh `run_id`** and folds the pinned
  planning run id into this run's `parent` facet (job name + namespace carried
  over). Execute through a session decorated with that context. **START** (at
  physical planning, inside `do_get`) and **COMPLETE/FAIL** (emitted by
  `OpenLineageExec` at stream end) then share *this execution's* run id, while
  the parent chain ties the run back to the statement it came from.

### Why a fresh run id per execution (refinement)

A prepared statement may be executed many times under one handle. Reusing one
pinned run id across executions violates exactly-once: N executions emit N
START + N terminal events under the **same** `runId`, and a FAIL from execution 2
can clobber a COMPLETE from execution 1 in any latest-event-wins state store.
Minting a fresh run id per execution keeps each run's START/terminal pair
exactly-once; correlation back to the statement (and onward to any orchestrator
parent, promoted to `root`) is preserved through the parent facet rather than a
shared id. This stays consistent with the OpenLineage run/parent model and needs
no change to the OpenLineage crate.

## Consequences

- Within one execution, START and COMPLETE/FAIL share one `runId`
  (`session::integration_tests::lineage_start_and_complete_share_run_id`,
  `engine::tests::pinned_run_id_correlates_start_and_complete`).
- Re-executing one (prepared) statement yields **distinct** run ids per
  execution, each parented to the planning run
  (`engine::tests::prepared_statement_reexecution_uses_distinct_run_ids`).
- The statement store is **session-scoped** (on `Session`), not a global map, so
  handles are naturally isolated per connection and torn down with the session.
- Statements are removed on `do_get` completion (statement path) or
  `ClosePreparedStatement` (prepared path), with a TTL sweep backstopping handles
  that are minted but never fetched.
- `run_id` and `sql` live inside the `LineageContext` snapshot rather than as
  separate `StoredStatement` fields, keeping one source of truth.
