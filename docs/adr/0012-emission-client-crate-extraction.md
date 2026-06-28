# 0012 — Engine-agnostic emission client crate (`openlineage-client`)

> Status: **Accepted** (2026-06). The OpenLineage emission model, the `Transport`
> seam, and the non-blocking `OpenLineageClient` live in `crates/openlineage-client`
> (no engine dependency). `datafusion-openlineage` depends on it, re-exports its
> surface, and adds the DataFusion glue. Builds on the planner design in ADR
> [0005](0005-openlineage-planner-vs-rule.md).

## Context

The OpenLineage **emission** side — the `RunEvent`/facet model, the pluggable
`Transport` sink, and the non-blocking client that drains events on a background
task — originally lived entirely inside `datafusion-openlineage`. Anything that
wanted to emit OpenLineage events (a future Kafka transport, a non-DataFusion Rust
emitter) would have had to depend on `datafusion-openlineage`, dragging in the
whole DataFusion dependency tree just to reach `RunEvent` and `Transport`.

We also wanted the emission seam to stay genuinely unopinionated about *how* events
are published: a target might be a spec-compliant OpenLineage REST API, a Kafka
topic, or something else. The integration must not prescribe transport.

Scope note: "client" here means the **ingest/emit** side only. There is no
OpenLineage spec for the read path, so the read API stays entirely inside the
`headwaters` service and is not a concern of these crates.

## Decision

Extract one engine-agnostic crate, **`openlineage-client`**, owning the event model
(`event`, `facets`, `naming`), the `Transport` trait + built-in transports
(`Noop`, `Console`, and the `http`-gated `CloudClientTransport`), the
`OpenLineageClient` queue/drain machinery, `OpenLineageConfig`, and the
`LineageContext` data + its env conventions. `datafusion-openlineage` depends on
it, re-exports its surface (so flat `datafusion_openlineage::{RunEvent, …}` paths
keep working), and keeps only the DataFusion-specific glue: the query/extension
planner, the exec node, lineage extraction, and the `LineageContextProvider` trait
(whose method takes a `SessionState`).

**One crate, not the OpenTelemetry three-way (API / SDK / exporter) split.** Every
consumer that wants the event model also wants the `Transport` trait and the
client; there is no population that wants one without the others, so an API/SDK
split would be two always-co-depended crates — ceremony for no gain at this scale.
The module boundaries keep a later split clean if a pure-serde consumer ever
appears.

**Three `RunEvent` representations stay separate** (not unified): the emission
model here (hand-authored serde, typed facets), the ingest/storage model
(`lineage.v1.RunEvent`, buffa-generated, with `raw_json` + opaque `Struct` facet
bags for lossless round-trip of arbitrary producers — see ADR
[0010](0010-read-api-proto-source-of-truth.md)), and the spec itself. They connect
only via JSON on the wire, which is correct: forcing the emitter to share a struct
with the proto model would drag a proto runtime into the integration and break the
unopinionated seam.

## Consequences

- A future `openlineage-kafka` (or any non-DataFusion emitter) depends only on
  `openlineage-client` — no DataFusion in its tree. This is verified in CI by
  asserting `cargo tree -p openlineage-client` contains no `datafusion`.
- Two crates publish to crates.io (client first, since the integration depends on
  it); `release-plz.toml` orders them. (Each crate's one-time first publish, which
  predates crates.io Trusted Publishing, is a manual token publish — see CONTRIBUTING.)
- `OpenLineageConfig` is engine-neutral; the DataFusion engine identity moves
  behind a `DataFusionConfig` extension trait (`for_datafusion`).
- The `Transport` trait gained defaulted `emit_batch` and `flush` methods. `flush`
  is invoked on `shutdown`, closing the tail-loss gap for transports that buffer
  internally (e.g. Kafka). Simple transports override neither.
