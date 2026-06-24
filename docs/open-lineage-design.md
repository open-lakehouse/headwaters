# OpenLineage on DataFusion — technical design

> A concise record of the design decisions and patterns behind the
> `datafusion-open-lineage` crate. Source material for a blog post on how we
> extend DataFusion to build a full-featured, governed object-store query
> service.

## Why

We run a Flight SQL query service (`hydrofoil`) on Apache DataFusion over object
storage and Delta/Unity Catalog tables. A query engine in a lakehouse isn't
finished when it returns rows — it has to tell the rest of the platform *what it
touched*: which datasets were read, which were written, and how columns flow
between them. That's [OpenLineage](https://openlineage.io): an open standard for
lineage events that catalog and observability tools (Marquez, DataHub, …)
already consume.

The goal: emit spec-conformant lineage from DataFusion **without forking the
engine**, as a crate that drops into any `SessionContext`, with the wire
transport and the orchestration context both pluggable. This mirrors how we
already extend DataFusion for Cedar authorization and Unity Catalog resolution —
lineage is the third capability layered onto the same session.

## Design principles

1. **Plug in, don't fork.** Use DataFusion's published extension points; ship a
   standalone crate, not a patched engine.
2. **Conform to the ecosystem.** Match the OpenLineage object model and naming
   spec exactly — interoperability is the whole point.
3. **Lineage must never break the query.** Emission is best-effort, async, and
   swallows its own errors.
4. **Pluggable at the edges, opinionated in the middle.** The transport (where
   events go) and the context (orchestration metadata) are traits; the model and
   the plan extraction are fixed and correct.
5. **Learn from prior art, defer the rest.** Adopt the patterns mature
   integrations (Spark) have validated; skip the machinery that doesn't earn its
   complexity in a first version.

## Critical decision 1 — Where to hook: a `QueryPlanner` + a registered `ExtensionPlanner`

DataFusion offers several extension seams: `AnalyzerRule`, `OptimizerRule`,
`PhysicalOptimizerRule`, custom `ExecutionPlan` nodes, the `ExtensionPlanner`, and
the `QueryPlanner` trait. The work splits along three concerns with different
needs (see ADR [0014](adr/0014-openlineage-planner-vs-rule.md)):

- **Extraction** needs the *fully optimized* `LogicalPlan` — the richest lineage
  signal (scans have projections/filters pushed down, so we see exactly which
  columns are read).
- **START + orchestration context** need `&SessionState` and are async.
- **Terminal COMPLETE/FAIL + runtime stats** need a node at the physical root that
  observes execution.

`QueryPlanner::create_physical_plan(&LogicalPlan, &SessionState)` is the only
logical-phase seam that gets `&SessionState`, so the first two concerns live in an
`OpenLineageQueryPlanner`. But the terminal node is installed the composable,
DataFusion-idiomatic way — a **registered `ExtensionPlanner`**, not the planner
hand-wrapping the physical root. The planner carries a prebuilt COMPLETE template
through the plan itself in a `LineageMarker` (`UserDefinedLogicalNodeCore`); a
`LineageExtensionPlanner` lowers that marker into `OpenLineageExec` at the root.
(The plan is the only per-query carrier from the logical phase into physical
planning — rules get no `&SessionState` and no per-query mutable channel.)

```rust
// OpenLineageQueryPlanner: extract + context + START, then wrap the *logical*
// plan in a LineageMarker carrying the COMPLETE template; delegate physical
// planning to a DefaultPhysicalPlanner that registers LineageExtensionPlanner.
let wrapped = LogicalPlan::Extension(Extension { node: Arc::new(marker) });
self.physical.create_physical_plan(&wrapped, session_state).await
```

This composes cleanly with our other customizations — the Cedar policy check and
the `datafusion-tracing` physical-optimizer rule coexist on one session, and any
host-registered extension planners are preserved.

**Public surface** is one call, matching `datafusion-tracing`'s ergonomics:

```rust
let state = instrument_session_state(state, client, context_provider, config);
```

### Two hooks: START at plan time, COMPLETE/FAIL at execution time

The planner emits only **START** — at this point we have the optimized plan and
the context, which is everything START needs. But COMPLETE/FAIL must reflect the
actual *execution* outcome, not just that planning succeeded: a query that plans
cleanly can still error mid-stream. Emitting COMPLETE from the planner would
report success for a query that later fails.

So the planner carries the pre-built COMPLETE event (same `runId` as START)
through the plan in a `LineageMarker`, which a registered `LineageExtensionPlanner`
lowers into an **`OpenLineageExec`** node at the physical root. That node observes
the result streams and emits the terminal event when execution actually ends:

- COMPLETE when every output partition drains successfully;
- FAIL (with an `errorMessage` run facet) if any partition yields an error, or
  is dropped before exhaustion (a cancelled/abandoned query).

**Exactly-once across partitions.** A root plan has *N* output partitions, each
producing an independent stream. `OpenLineageExec` shares a small `RunState`
(an `AtomicUsize` of outstanding partitions + a `failed` flag) across them. Each
partition's stream is wrapped so that on terminal — exhaustion, error, or
`Drop` — it decrements the counter; the partition that brings it to zero emits
the single terminal event. Using `Drop` as the completion signal means
cancellation is handled without special-casing, and an `emitted` flag guards the
zero-partition edge case. This is the same `Drop`-based technique
`datafusion-tracing` uses to harvest metrics, specialized here for once-per-run
event semantics rather than per-stream metric aggregation.

A *planning* failure is different: there's no execution to observe, so the planner
emits FAIL directly.

### Runtime statistics, and where the row count actually lives

The COMPLETE event also carries an **`outputStatistics`** facet (`rowCount`,
`size`) per output dataset — the runtime "how much did we write." The obvious
source is the inner plan's native `MetricsSet`, the way `datafusion-tracing`
harvests metrics on completion. But for the case that matters here — a write
(`INSERT`/`CTAS`) — the root `DataSinkExec` returns `None` from `metrics()`; the
rows-written count isn't there. DataFusion instead reports it *in the result
stream*: a write yields a single batch with one `count` (UInt64) column whose
value is the number of rows written.

So `OpenLineageExec` watches the batches flowing through each partition's
stream, recognizes that `count`-batch shape, and accumulates the rows-written
total in `RunState`; `size` falls back to a `bytes_scanned`-style plan metric
when the plan exposes one. A read (`SELECT`) has no output dataset, so the facet
is simply absent. This is a good example of "read the engine, not the docs": the
authoritative signal was in the data path, not the metrics API.

### Input statistics, and the attribution wall

There is a symmetric **`inputStatistics`** facet (rows/bytes *read* per input
dataset). Three things make it trickier than output stats:

1. **The metrics are per-node, not on the root.** `ExecutionPlan::metrics()`
   returns only that node's own metrics (the root often reports `None`); it is
   not a recursive aggregate. So we walk the executed plan tree after completion
   and sum the scan nodes' `output_rows` + `bytes_scanned`. To avoid
   double-counting `output_rows` from intermediate nodes, we only count rows on
   nodes that also expose `bytes_scanned` — i.e. leaf file scans.
2. **Only file sources emit these metrics.** In-memory and CSV scans report
   neither `output_rows` nor `bytes_scanned`; `bytes_scanned` is Parquet-specific
   today. So `inputStatistics` appears for Parquet-backed reads and is absent
   otherwise — correctly, rather than guessing.
3. **Attribution is the real wall.** A summed scan total can only be attached to
   *an* input dataset, not split across several, unless we can match each scan
   node back to the dataset it reads. So we attach `inputStatistics` **only when
   the query has exactly one input dataset** (unambiguous). Multi-input queries
   get no input stats rather than a misleading aggregate.

**Deferred — per-dataset input attribution.** Splitting scan stats across
multiple inputs requires matching each `DataSourceExec` (which carries an
`object_store_url` + `file_groups`, i.e. physical paths) to the corresponding
input `Dataset`. Today our datasets are named from the *logical* `TableReference`
(location-based naming and the `symlinks` facet are themselves deferred), so
physical scan paths don't line up with dataset names. The clean sequencing is:
do **location-based dataset naming first** (object-store URL + `symlinks`), which
is independently valuable, then per-dataset input attribution becomes a small
addition on top. Attempting fuzzy path↔name matching before that would be
fragile, so we ship the single-input case and log the boundary instead.

## Critical decision 2 — Deriving lineage from the logical plan

Lineage extraction is a `TreeNodeVisitor` walk over the optimized
`LogicalPlan` — the exact pattern our Cedar integration uses to find tables to
authorize, which keeps the two readable side by side.

- **Inputs**: every `TableScan` → an input dataset, with a `SchemaDatasetFacet`
  built from the scan's `projected_schema`.
- **Outputs**: `Dml` (`Insert`/`Update`/`Delete`/`Ctas`) and
  `CreateExternalTable` → output datasets.
- **Schema**: every input carries a `SchemaDatasetFacet` built from the table's
  *full* schema (`scan.source.schema()`), not the projected scan schema — so a
  dataset's reported schema is stable across queries that read different column
  subsets after projection pushdown.

Inputs are deduped by `(namespace, name)`: a self-join scans one table twice but
is a single input dataset.

### Column lineage: positional bottom-up resolution (`src/column.rs`)

Column-level lineage is resolved by a separate bottom-up walk of the optimized
plan and attached to the **output** datasets, keyed by output field — the side
the spec defines `ColumnLineageDatasetFacet` on. (An earlier name-based,
top-down extraction was removed as unsound: aliases/CTEs fabricated datasets,
same-named projections clobbered each other, and the facet sat on inputs where
consumers never look. See ADR 0013 for the full decision record.)

The soundness keystone is **positional indexing**: every node's map is one
entry per output-schema field (*position → set of physical `(dataset, column)`
sources*), never keyed by name. Column refs in expressions resolve to child
positions via `DFSchema::maybe_index_of_column`, so qualifiers participate
exactly as DataFusion's own scoping does — aliases/CTEs/self-joins can neither
collide nor fabricate datasets, and identity provenance survives stacked
projections. Only the **root** map is published; for DML the SQL planner's
positional alignment of the input with the target schema keys the facet by the
target table's field names.

How sources are classified:

- `DIRECT/IDENTITY` — a bare column chain (through aliases) end to end;
  `DIRECT/TRANSFORMATION` — any other expression; `DIRECT/AGGREGATION` —
  aggregate and window-function arguments. Kinds max-merge along the plan.
- `INDIRECT/{FILTER, JOIN, GROUP_BY, SORT, WINDOW}` — predicate, join-key,
  group-key, sort-key, and window-key columns shape the output rows without
  flowing into them. They union upward and are appended to **every** output
  field's `inputFields` (matching the Spark integration's emission).
- `masking` is always `false` — masking detection is out of scope.

**Degradation policy.** Any unhandled node (`Extension`, surviving `Subquery`,
recursive CTEs), arity mismatch, unresolvable column ref, or expression
embedding a subquery drops the **whole facet** for the statement, with a
`tracing::debug!` line. A partially-correct per-column facet is
indistinguishable from a complete one, so whole-facet drop is the only honest
partial failure; there is deliberately no name-based fallback. Table-level
lineage is unaffected.

Known gaps (acceptable, recorded): `COPY TO` emits no lineage at all (also at
table level); recursive CTEs degrade (their work-table scan would fabricate a
dataset — the table-level extraction shares this latent issue); pure SELECTs
carry no column lineage because there is no output dataset to attach it to (a
synthetic "query result" dataset would pollute the graph and break the
no-inputs/no-outputs suppression).

### No silent truncation

Plan nodes we don't yet extract from log a `trace` line rather than vanishing —
coverage gaps stay visible, per OpenLineage's "don't silently drop" guidance.

## Critical decision 3 — Spec conformance is non-negotiable

Lineage is only useful if other tools accept it, so we model the OpenLineage
object spec precisely with `serde` types rather than approximating it:

- The six `eventType` values, a constant `runId` (UUIDv7) across START/COMPLETE/
  FAIL, top-level `producer` + `schemaURL`.
- Every facet embeds a `BaseFacet { _producer, _schemaURL }` (the underscore
  prefix is mandated to avoid collisions) pointing at versioned spec URLs.
- The facets a query engine is expected to populate: `processing_engine` (engine
  name/version), `schema`, `dataSource`, `sql`, `jobType`, `columnLineage` (on
  outputs — see *Column lineage* above), plus `parent` and `errorMessage`.
  `processing_engine.version` reports the **DataFusion** version (the engine),
  while `openlineageAdapterVersion` reports this crate's version.

Two `serde` details that bit us and are worth calling out:

- `#[serde(rename_all = "camelCase")]` turns `schema_url` into `schemaUrl`, but
  the spec wants `schemaURL` — so it needs an explicit `#[serde(rename)]`.
  Conformance means matching the spec's casing, not Rust's idiom.
- Facet bags carry `#[serde(flatten)] extra: Map<String, Value>` so
  context-supplied custom facets merge into the event without the crate
  enumerating every possible facet.

There is **no official OpenLineage Rust client** (only Java/Python/Go), so we own
this model — which is also why getting it exactly right matters.

### Naming-spec mapping

Dataset `(namespace, name)` follows the OpenLineage naming spec — this is what
lets graphs join across tools. Object-store URLs map as
`s3://bucket/key → namespace s3://bucket, name key`; `file:///p → namespace
file, name /p`. Because a bare `TableScan` carries only a `TableReference` (not a
storage location), v1 names from the qualified table reference and reserves the
`symlinks` facet for publishing the physical path + catalog identity once the
host integration provides it — the same physical-vs-catalog split Spark uses.

## Critical decision 4 — Pluggable transport, async, fail-safe

The sink is a trait, named to match OpenLineage's own SPI:

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError>;
}
```

Defaults: `NoopTransport` (the safe default when lineage isn't configured) and
`ConsoleTransport` (dev/test). The production default, behind the `http` feature,
is `CloudClientTransport` — built on our `olai-http` `CloudClient`, which already
handles bearer-token, Databricks, and cloud-credential auth. Emitting a lineage
event is then just `client.post(endpoint).json(event).send()`, and we inherit
auth against deployed, secured services for free. Keeping it feature-gated means
the core crate has no HTTP dependency.

**The fail-safe guarantee lives in `OpenLineageClient`.** `emit` is non-blocking:
it `try_send`s onto a bounded channel and, if the queue is full or closed, drops
the event with a warning. A background task drains the channel and calls the
transport, swallowing and logging any error. The query path never blocks on
lineage and never fails because of it — the Rust/`tokio` equivalent of Spark's
`asyncTaskQueue` circuit breaker. We read the standard `OPENLINEAGE_URL` /
`OPENLINEAGE_API_KEY` / `OPENLINEAGE_ENDPOINT` env vars so the integration
behaves the way operators already expect.

## Critical decision 5 — Pluggable orchestration context

A query's lineage often belongs to a larger run — an Airflow DAG task, a
Databricks job. OpenLineage models this with a `ParentRunFacet`. But *how* that
context arrives differs per orchestrator, so we don't hardcode it; we inject it:

```rust
#[async_trait]
pub trait LineageContextProvider: Send + Sync {
    async fn context(&self, session_state: &SessionState) -> LineageContext;
}
```

`LineageContext` carries optional `run_id`, job identity, a `parent_run` facet,
and free-form `run_facets`/`job_facets` maps — open extension points so an
integration attaches whatever its orchestrator provides. The provider is `async`
and receives the `SessionState`, which is the channel by which per-request data
reaches planning.

### The context-forwarding problem (and the bridge)

There is **no standard OpenLineage header** for forwarding parent context — every
integration uses native config (Spark properties, env vars). For our Flight SQL
service we define our own gRPC metadata keys, mirroring Spark's discrete
`parent*` property names (`x-openlineage-parent-run-id`, …; slash-safe, unlike
the single `{ns}/{name}/{runId}` form).

The architectural seam: `LineageContextProvider::context()` only sees a
`SessionState`, but the headers arrive on the gRPC request. The bridge is a
**typed `SessionConfig` extension** — the request handler parses metadata into a
`LineageContext`, attaches it via `SessionConfig::with_extension`, and the
provider reads it back with `get_extension`. DataFusion internals stay unaware of
gRPC; orchestration context still flows to planning.

## Where the events fire, and the session question

Because we hook *physical* planning, the run's identity is intrinsically
self-consistent: START and the `OpenLineageExec` that emits COMPLETE/FAIL both
originate from a single `create_physical_plan` call, so they share one `runId`
without any external bookkeeping. In hydrofoil today that call happens inside the
`do_get_*` RPC's streaming task (`get_flight_info_statement` only builds the
*logical* plan and hands back a handle), so the whole START→COMPLETE/FAIL
lifecycle lives in one task — the physical-plan wrapper "just works" here.

The session question is about a *different* axis: a client's logical operation
spans several RPCs (`CreatePreparedStatement` → `GetFlightInfo` → `DoGet` →
`Close`), and we'll eventually want one lineage run per logical operation, plus
per-session orchestration context that outlives a single statement. That needs
real session/statement management — a session keyed by a protocol-derived id
(Flight SQL handshake / cookie), statements owning their `runId` — replacing the
current demo `get_ctx` stub. That work is designed in `docs/session-management.md`
and intentionally deferred; the lineage layer is already structured to slot into
it (the `LineageContextProvider` reads context from the session, and a statement
could pre-mint the `runId` the planner uses).

## Patterns adopted vs. deferred

**Adopted** (validated by mature integrations, mostly Spark): a `QueryPlanner` for
the `&SessionState`-bound planning-time work, with the terminal node installed via
a registered `ExtensionPlanner`; `TreeNodeVisitor` plan walk; table-level
input/output lineage with
full-schema facets; spec-exact facets with `_producer`/`_schemaURL`; async
fail-safe emission with a flush-on-shutdown drain and a dropped-events counter;
standard env-var config; pluggable transport; **end-of-execution COMPLETE/FAIL
via a `Drop`-based physical-plan wrapper** (`OpenLineageExec`), so terminal events
reflect runtime outcome, not just planning success; **`outputStatistics`
facets** (rows written, from the write-result `count` batch); **`inputStatistics`
facets for single-input reads** (rows/bytes scanned, summed from the plan tree's
file-scan nodes).

**Deferred** (complexity that doesn't earn its place yet): **column-level
lineage** — until a sound, scope-aware extraction exists (see *Column lineage is
deferred* above); **per-dataset input
attribution** for multi-input queries — needs location-based dataset naming
(object-store URL + `symlinks`) first, so scan nodes can be matched to datasets
(see *Input statistics* above); the full
`OpenLineageEventHandlerFactory` ServiceLoader-style SPI for third-party facet
builders — Rust has no ServiceLoader, and a fixed visitor set plus trait seams
covers us until a second consumer appears; Kafka/GCS/composite transports;
JVM-specific circuit breakers (our bounded queue covers the safety goal); richer
input-side statistics (bytes/files scanned per source); real session/statement
management for cross-RPC, per-operation runs.

## The bigger picture

OpenLineage is one of three capabilities we layer onto the same DataFusion
session through its extension points — alongside **Cedar policy enforcement**
(authorize the optimized plan before execution) and **Unity Catalog resolution**
(resolve `catalog.schema.table` and vend object-store credentials at plan time).
The recurring pattern is the same: *intercept at planning, read the
`LogicalPlan`/`SessionState`, compose by wrapping rather than replacing.* That's
how you build a full-featured, governed object-store query service on DataFusion
without forking it — which is the story this design is one chapter of.
