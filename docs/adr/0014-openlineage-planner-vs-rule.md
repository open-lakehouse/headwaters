# 0014 — OpenLineage installs its terminal node via a registered `ExtensionPlanner`

> Status: **Accepted** (2026-06). Implemented in `crates/open-lineage/src/rule.rs`
> (the `LineageMarker` node, `LineageExtensionPlanner`, and
> `OpenLineageQueryPlanner`), wired by `src/session.rs`
> (`instrument_session_state`). Refines the "Critical decision 1 — Where to hook"
> section of [`docs/open-lineage-design.md`](../open-lineage-design.md) and
> complements ADR [0003](0003-per-statement-run-id-correlation.md) (per-statement
> `run_id` correlation).

## Context

`datafusion-tracing` instruments a session *without* a bespoke session type: it
registers a `PhysicalOptimizerRule` (`with_physical_optimizer_rule`) that wraps
each physical node in an `InstrumentedExec` opening a span around its own stream.
That registration style is composable and idiomatic. The original lineage
integration instead wrapped the whole `QueryPlanner` and hand-wrapped the physical
root in `OpenLineageExec`. We evaluated moving to the registered-extension style.

Lineage instrumentation has **three concerns with different needs**:

| Concern | Needs | Available to a rule? |
| --- | --- | --- |
| Table/column lineage extraction | the optimized **`LogicalPlan`** (`src/extract.rs`, `src/column.rs`) | A `PhysicalOptimizerRule` sees the *physical* tree, where column provenance is lossy/absent. |
| START event + orchestration context | `&SessionState`, **async** (`LineageContextProvider::context`) | `AnalyzerRule`/`OptimizerRule` receive only an immutable `&ConfigOptions`/`&dyn OptimizerConfig` — **no `SessionState`**, and they are synchronous. |
| Terminal COMPLETE/FAIL + runtime stats | a node at the physical root that observes all partitions finishing | **Yes** — installable by a registered extension at physical-planning time. |

Two DataFusion 53.1 facts (verified against source) shape the design:

- `SessionState::create_physical_plan(&self, …)` runs the physical optimizer rules
  against the **session-shared, immutable** `config_options()`. There is no
  per-query mutable channel between phases, so a `run_id`/template minted at
  planning cannot be handed to a later physical rule through a side-channel (and a
  shared slot would race across concurrent queries on a session).
- `QueryPlanner::create_physical_plan(&LogicalPlan, &SessionState)` is the **only**
  logical-phase extension point that receives `&SessionState`, so it is the only
  place the async context provider can run and a per-query `run_id` can originate.

The only carrier of per-query state from planning into the physical phase is the
**plan itself**.

## Decision

Split the work along the three concerns and install the terminal node the
composable way — via a **registered `ExtensionPlanner`** rather than the planner
hand-wrapping the physical root:

1. `OpenLineageQueryPlanner` (a `QueryPlanner`, the only seam with `&SessionState`)
   extracts lineage from the optimized `LogicalPlan`, resolves the async context,
   emits START, mints the `run_id`, emits FAIL on a planning error, and wraps the
   *logical* plan in a `LineageMarker` (`UserDefinedLogicalNodeCore`) carrying the
   prebuilt COMPLETE template.
2. `LineageExtensionPlanner` (an `ExtensionPlanner`, registered on a
   `DefaultPhysicalPlanner`) lowers that marker into `OpenLineageExec` at the root,
   which emits COMPLETE/FAIL at end of execution under the same `run_id`.

A spike (since productionized into the implementation) confirmed the marker
survives optimization + physical planning to the extension planner, and that the
event stream is identical to the previous hand-wrapping path across the SQL matrix
(read / aggregate / join / INSERT-with-derived-column / runtime-failure), with the
full conformance suite passing.

A `LogicalPlan::Extension` requires a registered `ExtensionPlanner` (the default
physical planner errors on unknown extension nodes), so the planner delegates
physical planning to a `DefaultPhysicalPlanner::with_extension_planners`, composing
any extension planners the host already had.

## Consequences

- **The integration reads as extension registration, not planner replacement** —
  the terminal node is installed by a standard `ExtensionPlanner`, matching the
  `datafusion-tracing` composability goal. The `QueryPlanner` remains only for the
  irreducibly `&SessionState`-bound, async planning-time work (extraction, context,
  START); it no longer hand-wraps the physical plan.
- The public surface is unchanged: `instrument_session_state` /
  `instrument_session_state_simple` keep the same names and signatures, so the sole
  consumer (`crates/hydrofoil/src/session/mod.rs`) needs no change. The old
  root-wrapping `OpenLineageQueryPlanner`/`planner.rs` is removed.
- The OTel/`datafusion-tracing` *physical-optimizer-rule* pattern fits stateless,
  node-local execution spans; lineage's START/context half is run-scoped and
  session-dependent, so it stays at the `QueryPlanner`. Only the terminal-node half
  is genuinely rule-shaped, and that is the half now installed by registration.
- **Revisit trigger:** if a future DataFusion exposes `&SessionState` (or a
  per-query mutable context) to analyzer/optimizer rules, the START/context half
  could also move off the `QueryPlanner`; re-open this then.
