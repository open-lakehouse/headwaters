# 0011 — Envoy → PostgreSQL lineage via a custom `proxy-wasm` filter

> Status: **Proposed** (2026-06). Design-stage decision; no implementation
> shipped. Full design in
> [`docs/envoy-postgres-lineage-design.md`](../envoy-postgres-lineage-design.md).
> Adapts [ADR 0001](0001-per-statement-run-id-correlation.md) (run identity),
> [ADR 0003](0003-client-forwarded-lineage-metadata.md) (client-forwarded
> metadata) and [ADR 0004](0004-column-level-lineage-positional-resolution.md)
> (column-lineage soundness) to a non-DataFusion source; feeds the pipeline of
> [ADR 0006](0006-hybrid-cqrs-postgres-storage.md).

## Context

Headwaters emits rich column-level OpenLineage from DataFusion query plans. We
want a **second source** for plain PostgreSQL traffic that does not run on
DataFusion: instrument an Envoy proxy in front of Postgres so SQL flowing through
it emits OpenLineage into the existing `headwaters` ingest API.

The granularity bar is **tables and (best-effort) columns**, plus the query text
— not just the coarse `table.db → operation` signal available off the shelf. The
constraint that drives everything: to get tables and columns we need the **SQL
text**, and to parse it we need a parser we control.

## Decision

Build a **custom Envoy network (L4) filter in Rust using
[`proxy-wasm-rust-sdk`](https://github.com/proxy-wasm/proxy-wasm-rust-sdk)** that:

1. Uses the SDK's `StreamContext` (`on_downstream_data` / `on_upstream_data`) to
   decode the Postgres frontend wire protocol (Simple Query `Q`, Extended
   `Parse`/`Bind`/`Execute`) and recover **SQL text** plus per-connection
   context.
2. Parses the SQL **in-filter** with the pure-Rust, `wasm32`-friendly
   [`sqlparser`](https://crates.io/crates/sqlparser) crate (PostgreSQL dialect)
   to extract input/output tables and best-effort columns. Column lineage follows
   the ADR 0004 **degradation policy**: drop the `columnLineage` facet for the
   whole statement on any ambiguity (`SELECT *`, unparseable input, unhandled
   construct) rather than emit a guess.
3. Builds OpenLineage `RunEvent`s (one run per statement, `run_id = UUID v7`,
   mirroring ADR 0001), naming Postgres datasets as namespace
   `postgres://{host}:{port}` / name `{database}.{schema}.{table}` consistent
   with `crates/open-lineage/src/naming.rs`.
4. **Batches and `dispatch_http_call`s** events to the existing
   `headwaters` `POST /api/v1/lineage/batch` endpoint — `headwaters` is
   **unchanged**.

Two supporting decisions:

- **Extract a DataFusion-free `crates/open-lineage-events` crate.** Today
  `crates/open-lineage` hard-depends on `datafusion` (via `builder.rs` /
  `extract.rs` / `context.rs`), so it cannot be compiled into a Wasm module. The
  struct definitions (`event.rs`, `facets.rs`, `naming.rs`) are already
  DataFusion-free; move them into a new crate depending only on
  `serde`/`serde_json`/`uuid`/`chrono`/`url`, and have `crates/open-lineage`
  re-export from it. The filter depends only on this new crate. This is the key
  refactor that makes the integration genuine reuse rather than copy-paste.
- **Run correlation rides a Postgres-native channel.** Postgres has no
  `x-openlineage-*` header equivalent (ADR 0003). Recommend a **leading
  SQL-comment convention** (`/* openlineage: parent=ns/name/runId; job=... */`)
  parsed by the filter — per-statement, pooling-safe, no Envoy header plumbing —
  with `application_name` as a coarser connection-scoped fallback.

A deployment requirement, not a choice: **Envoy must terminate downstream TLS**
so the filter sees plaintext. End-to-end-encrypted traffic past Envoy is opaque.

## Alternatives considered

- **Stock `envoy.filters.network.postgres_proxy` + gRPC ALS sidecar.** Lowest
  effort: consume the filter's dynamic metadata (`AccessLogCommon.metadata`) via
  a TCP gRPC Access Log Service implemented in Rust. **Rejected as primary** —
  the metadata is `table.db → operation` only: no query text, no columns, and the
  filter is blind on SSL. Noted as a viable *fast MVP* if table+operation lineage
  alone is ever acceptable.
- **Standalone Rust sidecar fed by a custom C++ filter or Envoy tap.** Would
  permit `datafusion-sql` for higher-fidelity parsing, but adds a non-Rust/Wasm
  build surface and a second process. **Rejected** for the data-path role;
  retained as the parsing backend in the hybrid below.
- **`proxy-wasm` filter forwarding raw SQL to a parsing sidecar (hybrid).** Thin
  data-path filter (decode + correlation) + companion sidecar doing the heavy
  parse/enrichment. **Deferred to Phase 2** — the natural evolution if in-VM
  parsing proves too costly at high QPS.
- **`datafusion-sql` inside the Wasm filter.** **Rejected** — DataFusion does not
  compile into a lean `wasm32` module; `sqlparser` is the Wasm-viable parser.

## Consequences

- A new lineage source lands events into the *unchanged* downstream half of
  Headwaters (ingest → projection → read API → UI); no `headwaters` changes.
- **Lower column-lineage fidelity than the DataFusion path** is accepted up
  front: no catalog means name-based, best-effort columns and no schema facets;
  the ADR 0004 degradation policy keeps it honest (tables reliable, columns
  dropped when uncertain).
- The `crates/open-lineage-events` extraction is required pre-work but is
  independently valuable — any future non-DataFusion emitter reuses it.
- **TLS termination at Envoy becomes a hard deployment constraint**; e2e-encrypted
  traffic is uninstrumentable, identical to the stock filter's limitation.
- **Correlation depends on client cooperation** (SQL comments); absent it, runs
  are unparented nodes. This is the weakest link and is validated first.
- **Wasm parsing cost at high QPS is unproven**; sampling, work caps, and the
  Phase-2 sidecar offload are the escape hatches.
- The whole filter is Rust → Wasm: same language as the rest of the repo, no
  forked Envoy, no C++ build.
</content>
