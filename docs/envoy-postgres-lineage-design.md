# Envoy → PostgreSQL OpenLineage integration — technical design

> A feasibility assessment and design for instrumenting an **Envoy proxy** in
> front of PostgreSQL so that SQL traffic emits OpenLineage events into the
> existing `headwaters` ingest API. This is a **design document** — no code
> ships with it; it exists to pick an architecture and decide whether to build.
> The decision summary lives in
> [ADR 0011](adr/0011-envoy-postgres-lineage-via-proxy-wasm.md).

## Why

Headwaters today produces **rich, column-level OpenLineage** from Apache
DataFusion query plans (the `datafusion-open-lineage` crate; see
[`open-lineage-design.md`](open-lineage-design.md)) and ingests / serves it
through a CQRS HTTP service (`headwaters`; see
[ADR 0006](adr/0006-hybrid-cqrs-postgres-storage.md)). That signal is
*plan-derived*: positional column lineage, full Arrow schemas, SQL text,
parent-run correlation.

We want a **second integration source** that does not require the workload to
run on DataFusion: a great deal of SQL in a platform is plain PostgreSQL traffic.
If an Envoy proxy already fronts Postgres (or can be inserted), we can observe
that traffic and emit lineage for it — feeding the *same* downstream half of
Headwaters (ingest → projection → read API → UI) with no backend changes.

### Goals

- Emit spec-conformant OpenLineage `RunEvent`s for SQL flowing through an Envoy
  proxy in front of Postgres.
- Recover **input and output tables**, and **best-effort column lineage**, by
  parsing the SQL text — not just the coarse table+operation signal the stock
  filter offers.
- Reuse `headwaters` ingest unchanged; reuse the OpenLineage event/facet
  model from `crates/open-lineage`.

### Non-goals

- **Schema-backed, provably-sound column lineage.** Without a catalog the proxy
  cannot resolve `SELECT *`, infer column types, or do positional resolution the
  way the DataFusion path does (ADR 0004). Column lineage here is name-based and
  best-effort, with an honest degradation policy.
- **Instrumenting end-to-end-encrypted traffic.** The proxy must be able to see
  plaintext Postgres bytes (see *TLS / deployment*).
- **Modifying or gating queries.** Lineage is observability, never admission
  control — malformed or unparsable SQL is skipped, never failed.

## Why the stock `postgres_proxy` filter is insufficient

Envoy ships a built-in L4 network filter,
`envoy.filters.network.postgres_proxy`. It is attractive (zero custom code) but
far below what Headwaters needs:

| Capability | Stock `postgres_proxy` | Headwaters needs |
|---|---|---|
| Granularity | dynamic metadata `table.db → [select\|insert\|update\|delete\|create\|drop\|alter\|show]` | input/output tables **+ columns** |
| Query text | not exposed | needed for the `sql` job facet and for parsing |
| SQL parser | best-effort; docs state it "does not successfully parse all SQL statements" | must recover input/output tables and column refs |
| TLS | **blind** — when a session is encrypted the filter ignores the messages and does no decoding | must see plaintext queries |
| Extended protocol | limited Parse/Bind/Execute handling | needed for prepared statements / real workloads |

Its dynamic metadata could be consumed out-of-band via Envoy's TCP gRPC Access
Log Service (`AccessLogCommon.metadata`), and that is a legitimate low-effort
MVP. But table+operation only, no query text, and no column lineage make it a
**non-starter as the primary path**. We record it as a rejected alternative in
the ADR.

The chosen path is a **custom Envoy network filter written in Rust via
`proxy-wasm`** that sees the raw Postgres wire bytes, extracts SQL text, and lets
us parse it ourselves.

## Why a custom `proxy-wasm` network filter is feasible

The [`proxy-wasm-rust-sdk`](https://github.com/proxy-wasm/proxy-wasm-rust-sdk)
exposes exactly the seams we need at L4 via its `StreamContext`:

- `on_new_connection`, `on_downstream_data`, `on_upstream_data`,
  `on_downstream_close` — raw byte access to each TCP frame in both directions.
  This lets us decode the Postgres frontend protocol and pull out SQL text.
- `dispatch_http_call(...)` — emit OpenLineage JSON to a `headwaters`
  upstream cluster asynchronously, off the data path.
- `on_tick` + shared queues — batch events and flush periodically into
  `headwaters`'s `/api/v1/lineage/batch` endpoint.

The whole filter is Rust compiled to a Wasm module that Envoy loads — no C++, no
forked Envoy build, and the same language as the rest of Headwaters.

## Architecture

```
Postgres client ──TCP──▶ Envoy (downstream TLS terminated here)
                          │
                          ├─ custom proxy-wasm network filter (Rust, StreamContext)
                          │     • decode PG frontend msgs (Q / Parse / Bind / Execute)
                          │     • extract SQL text + session/correlation context
                          │     • parse SQL via sqlparser (PG dialect) → tables (+ columns)
                          │     • build OpenLineage RunEvent JSON
                          │     • batch + dispatch_http_call → headwaters cluster
                          ▼
                       upstream Postgres
                          
headwaters  ◀── POST /api/v1/lineage/batch   (existing, UNCHANGED)
   events log → async projection → read tables → REST + ConnectRPC read API → UI
```

### Component 1 — the custom `proxy-wasm` filter (the new artifact)

**Postgres protocol decoding.** Decode enough of the frontend protocol to recover
SQL text:

- **Simple Query** (`'Q'` message): the SQL string is the message payload.
- **Extended Query**: `Parse` carries the SQL (and an optional prepared-statement
  name); `Bind` binds parameter values to that statement; `Execute` runs it. The
  filter keeps a small per-connection map of statement-name → SQL so a later
  `Execute` can be attributed to the right query.
- Handle TCP framing: a single `on_*_data` callback may contain partial or
  multiple Postgres messages; the filter buffers and reframes on the 1-byte type
  + 4-byte length header.
- On `SSLRequest` / encrypted sessions the filter must detect it cannot decode
  and bail gracefully (same posture as the stock filter — see *TLS*).

**SQL → lineage.** Parse the recovered SQL with the
[`sqlparser`](https://crates.io/crates/sqlparser) crate (`PostgreSqlDialect`),
which is pure Rust and compiles to `wasm32`. Walk the AST for:

- **Input tables**: `FROM` / `JOIN` relations, CTE sources, subqueries.
- **Output table**: `INSERT INTO`, `UPDATE`, `DELETE FROM`, `CREATE TABLE AS
  SELECT`, `CREATE MATERIALIZED VIEW`.
- **Columns** (best-effort): projection list → output columns; referenced
  columns in expressions → input columns.

**Column-lineage degradation policy.** Mirror the existing crate's honesty (ADR
0004): if the SQL cannot be parsed, references `SELECT *`, uses constructs the
walker doesn't soundly handle, or any output column's sources are ambiguous,
**drop the `columnLineage` facet for the whole statement** rather than emit a
guess. Table-level lineage can still be emitted when columns can't be resolved.

**Event construction.** Assemble an OpenLineage `RunEvent` (reusing the model —
see Component 2). One statement → one run: mint `run_id = UUID v7` per statement
(mirroring [ADR 0001](adr/0001-per-statement-run-id-correlation.md) semantics),
attach the `sql` job facet (the raw query text), the `processingEngine` facet
(name e.g. `envoy-postgres`, plus filter version), and the `parent`/correlation
facet from the chosen channel (see *Run correlation*). Because the proxy observes
the statement as a single unit, the simplest first version emits a single
`COMPLETE` event per statement; START/COMPLETE pairing keyed on the backend
`CommandComplete`/`ErrorResponse` is a later refinement that also yields FAIL
events and row counts.

**Dataset naming.** Apply the OpenLineage naming spec for Postgres, consistent
with `crates/open-lineage/src/naming.rs`:

- **namespace**: `postgres://{host}:{port}` (the upstream Postgres endpoint Envoy
  routes to — known from filter/cluster config, not from the query).
- **name**: `{database}.{schema}.{table}` — `database` from the startup message,
  `schema` defaulting to `public` when the table is unqualified.

This warrants a small `DatasetName::from_postgres(host, port, database, schema,
table)` helper added alongside the existing `from_location` / `from_table_ref`
constructors when the work is implemented.

**Emission.** Push finished events into a per-VM buffer; on `on_tick` (e.g. every
~1–2s or at a size threshold) serialize a JSON array and `dispatch_http_call` to
the `headwaters` cluster's `POST /api/v1/lineage/batch`. The batch endpoint
already returns `202 Accepted` with per-event partial-success semantics, so a few
unparsable events never sink a batch.

### Component 2 — a DataFusion-free event-types crate (recommended refactor)

Today `crates/open-lineage` has a **hard `datafusion` dependency**: `builder.rs`,
`extract.rs`, and `context.rs` all use `LogicalPlan` / `SessionState`. A Wasm
filter cannot pull DataFusion into a `wasm32` module, and shouldn't need to — it
only needs the event/facet structs.

The struct definitions themselves (`event.rs`, `facets.rs`, `naming.rs`) are
already DataFusion-free; only the *builder and extraction* layers are coupled.
The recommendation:

- Extract `event.rs` + `facets.rs` + `naming.rs` into a new crate
  `crates/open-lineage-events` whose only deps are `serde` / `serde_json` /
  `uuid` / `chrono` / `url` — all `wasm32`-friendly.
- Have `crates/open-lineage` depend on and re-export from it, so existing
  consumers (and the DataFusion builder/extract code) are unchanged.
- The Wasm filter (and any future non-DataFusion emitter) depends only on
  `open-lineage-events`.

This is the one piece of pre-work in the broader repo; it turns the integration
from copy-paste into genuine reuse and is independently valuable.

### Component 3 — `headwaters` (unchanged)

No changes. The ingest converter
(`crates/headwaters/src/ingest/converter.rs`) classifies events by field
presence and requires only `eventTime`, `run.runId`, `job.namespace`,
`job.name`; the routes (`crates/headwaters/src/http.rs`) are
`POST /api/v1/lineage` and `POST /api/v1/lineage/batch`, returning `202 Accepted`
with batch partial-success. Projection, the Mutation-IR pipeline (ADR 0007), and
the read API all process these events identically to DataFusion-sourced ones.

## Run correlation

Postgres has no per-request header channel analogous to Flight SQL's
`x-openlineage-*` metadata (ADR 0003). Correlation context (run id, parent run,
principal) must ride a Postgres-native channel. Options, weakest-to-strongest for
this use case:

1. **Leading SQL-comment convention (recommended).** The client prefixes
   statements with a structured comment, e.g.
   `/* openlineage: parent=ns/name/runId; job=my_pipeline */ SELECT ...`. The
   filter parses the comment before handing the SQL to `sqlparser`. Survives
   connection pooling (per-statement), needs no Envoy header plumbing, and is a
   widely used pattern (sqlcommenter). This is the lead recommendation.
2. **`application_name` GUC / session parameters.** Read from the startup message
   or `SET application_name`. Connection-scoped, so it can't vary per statement
   on a pooled connection — a coarser fallback.
3. **Connection-derived context only.** Use downstream principal / SNI / source
   address from Envoy stream info as a job-namespace hint when nothing else is
   present.

This adapts the *intent* of ADR 0003 (client-forwarded lineage metadata) to a
header-less protocol. It is the weakest part of the design and the first thing to
validate with a real client.

## TLS / deployment requirements

The filter only sees plaintext Postgres. Two consequences that must be explicit
in any deployment:

- **Envoy must terminate downstream TLS.** The filter then runs on the decrypted
  stream (optionally re-encrypting to the upstream Postgres). This is the
  supported topology.
- **If the client negotiates SSL end-to-end past Envoy, the bytes are opaque** —
  the filter is blind, exactly like the stock `postgres_proxy`. There is no way
  around this without TLS termination at the proxy.

## Performance considerations

- **Per-query SQL parsing on the data path costs Wasm-VM CPU.** Mitigations:
  parse asynchronously / off the hot path where the SDK allows; **sample** (parse
  a configurable fraction of statements); cap parser work (size/time limits, skip
  obviously-uninteresting statements like `SET`, `BEGIN`, `COMMIT`, simple
  `SELECT 1` health checks).
- **Sidecar offload (likely Phase 2).** Keep the data-path filter thin — decode
  SQL text + correlation, forward to a companion Rust **sidecar** that does the
  heavy `sqlparser` walk and event construction. The sidecar variant could even
  use `datafusion-sql` for higher-fidelity parsing, at the cost of a second
  process. This is the natural evolution if in-VM parsing proves too costly.
- **Batching** (Component 1) amortizes the HTTP egress cost and matches the batch
  ingest endpoint.

## Phased delivery plan

- **Phase 0 — Spike.** Custom `proxy-wasm` filter that logs decoded SQL text from
  Simple Query messages against a local Postgres behind Envoy. Proves the
  wire-decode + Wasm build toolchain end to end.
- **Phase 1 — Refactor.** Extract `crates/open-lineage-events` (DataFusion-free);
  `crates/open-lineage` re-exports it. Pure, fully-tested refactor.
- **Phase 2 — Table-level lineage.** Filter parses SQL with `sqlparser`, emits
  table-level `RunEvent`s to `headwaters`. Verified end to end through the
  read API and UI.
- **Phase 3 — Column-level (best-effort).** Add column-reference extraction with
  the strict degradation policy; extended-protocol / prepared-statement support.
- **Phase 4 — Correlation + hardening.** SQL-comment run-id convention,
  batching/sampling, the TLS-termination deployment guide, and conformance of
  emitted events against the vendored OpenLineage JSON Schemas (reuse the harness
  pattern in `crates/open-lineage/tests/conformance.rs`).

## Risks & open questions

- **Column-lineage fidelity** from raw SQL is structurally lower than the
  DataFusion path. Set expectations: tables reliable, columns best-effort.
- **Correlation channel** depends on client cooperation (SQL comments). Without
  it, runs are isolated nodes with no parent chain.
- **Wasm parsing cost** at high QPS is unproven — Phase 0/2 must measure it;
  sampling and the sidecar offload are the escape hatches.
- **Extended-protocol edge cases** (multi-statement, COPY, server-side cursors,
  pipelining) need explicit scoping; skip-and-count rather than mis-attribute.
- **Schema drift**: dataset names without schema qualification default to
  `public`, which may mis-name tables in non-`public` searches — revisit if it
  causes graph fragmentation.

## Related

- [`open-lineage-design.md`](open-lineage-design.md) — the DataFusion path this
  complements.
- [ADR 0001](adr/0001-per-statement-run-id-correlation.md) — run identity.
- [ADR 0003](adr/0003-client-forwarded-lineage-metadata.md) — client-forwarded
  metadata (the model the SQL-comment correlation adapts).
- [ADR 0004](adr/0004-column-level-lineage-positional-resolution.md) — the
  column-lineage soundness bar and degradation policy.
- [ADR 0006](adr/0006-hybrid-cqrs-postgres-storage.md) — the ingest/projection
  pipeline this feeds.
- [ADR 0011](adr/0011-envoy-postgres-lineage-via-proxy-wasm.md) — the decision
  record for this design.
</content>
