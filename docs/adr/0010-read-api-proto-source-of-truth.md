# 0010 — Read API modeled in protobuf; REST stays hand-written for now

> Status: **Accepted** (2026-06). The proto lives in
> `proto/headwaters/read/v1/{read,service}.proto` (package `headwaters.read.v1`,
> service `ReadService`); the OpenLineage-aligned event/facet model and ingest
> stay in `proto/lineage/v1` (`IngestService`). The hand-written REST server in
> `crates/headwaters/src/read/` remains the serving path. Builds on the
> CQRS read model in ADR [0006](0006-hybrid-cqrs-postgres-storage.md).

## Context

Headwaters serves a Marquez-compatible **read API** (namespace/job/dataset
browse, the lineage + column-lineage graph, the events feed, run facets, dataset
versions, tags, PII propagation, and time-bucketed activity stats). It is
consumed today by the Marquez web client and, going forward, by rich interactive
UI components we want to embed in **Hydrofoil** and stand up on their own — built
with the **workflows** stack (`@xyflow/react` + ELK, `buf`/ConnectRPC-generated
TypeScript clients).

Before building that UI we wanted a **single, typed source of truth** for the
read API shape, and to evaluate generating the REST server + typed Rust/TS/WASM
clients from protobuf via the **Trestle / olai-codegen** framework (the same path
the `golden-path-app` and `mangrove` projects use).

Two facts shaped the decision:

1. **The read API is not part of the OpenLineage spec.** It is Marquez-inspired
   but headwaters-owned, so it must not co-habit the spec-aligned `lineage.v1`
   package. We carved it into its own package, `headwaters.read.v1`, with its own
   `ReadService`; ingest (the spec `POST /lineage` surface) was split into a
   separate `lineage.v1.IngestService`.
2. **The serving contract is Marquez-byte-compatible.** The web client sends
   specific query-string keys (`?nodeId=`) and tolerates specific response shapes
   (bare arrays for stats, the `type` key, `inEdges`/`outEdges`, opaque facet
   bags). That contract is load-bearing for the existing UI.

We ran an exploratory `trestle generate` over the new proto to see how close
generated REST comes to that contract.

## Decision

**Model the read API in protobuf as the source-of-truth shape, but keep the REST
server hand-written for now.** Do not wire the Trestle codegen pipeline
(handler-trait generation, generated client crate, ConnectRPC stubs) into the
build yet.

Rationale: the generated **response bodies** already match the Marquez contract
(buffa serializes to camelCase with `type` / `simpleName` / `currentVersion` /
`inEdges` / `outEdges` correctly named, and `google.protobuf.Struct` carries the
opaque facet bags), but the generated **query/routing layer** diverges from the
contract in four concrete ways (below). Rather than break the wire contract or
fork our serving path around codegen gaps, we keep the hand-written server —
which already honors the contract exactly — and treat the proto as the canonical
shape that server, the future generated clients, and the UI all agree on. We
follow Trestle conventions (flat top-level routes, `google.api.*` annotations,
request/response message factoring) so the eventual switch to generated serving
is mechanical.

The four divergences are recorded as **candidate Trestle improvements** (Trestle
is a sibling checkout we can change). Once the read API is fully implemented and
the UI exists, we revisit generating the serving path + clients after addressing
these upstream.

### Candidate Trestle / olai-codegen improvements

Observed from `trestle generate` over `headwaters.read.v1` (debug-build,
trestle `cb92df5`). Each is a gap between generated REST and the hand-written
Marquez contract:

1. **Query-param JSON name is ignored.** A non-path scalar request field
   `node_id` generates a `Query<QueryParams { node_id }>` extractor and a client
   that appends `?node_id=` — but the Marquez UI sends `?nodeId=`. The generated
   query binding should honor the field's JSON (camelCase) name, the same way the
   response serializer does.
2. **`additional_bindings` is dropped.** `ListJobs`/`ListDatasets` declare a
   primary route plus an `additional_bindings` namespace-scoped route
   (`/api/v1/namespaces/{namespace}/jobs`). Only the primary route is generated;
   the scoped routes — and the path-vs-query binding of `namespace` they require
   — are not emitted.
3. **Non-path scalar request fields are required.** `limit` / `offset` /
   `namespace` generate as non-`Option` fields with no `#[serde(default)]`, so a
   bare `GET /api/v1/jobs` (no query string) fails extraction with 400. The
   contract treats these as optional with server defaults (`limit=100`,
   `offset=0`, `namespace` = all).
4. **No bare-array response mapping.** The stats endpoints return a bare JSON
   array (`[{date,count}, …]`); proto has no top-level repeated, so we wrapped
   them in `StatsResponse { repeated StatBucket buckets }`, which serializes as
   `{"buckets":[…]}`. A `response_body`-style mapping (project a single repeated
   field to the top-level array) would let generated REST match the contract.

(Items 1–3 are also relevant to `mangrove` and any Marquez-shaped API; item 4 is
niche.)

### ConnectRPC streaming — recommendation, not yet adopted

All read RPCs are intentionally **unary and REST-derivable** in this phase.
ConnectRPC server-streaming is the right tool for a few endpoints once a concrete
need is measured, but streaming RPCs **break `google.api.http` / REST
generation** — they live only on the ConnectRPC path (`ServiceStream<…>`), so
adopting them is a deliberate trade of REST-derivability for incremental
delivery. Candidates, in priority order:

- **`ListEvents`** — the event feed is large, append-only, and tailable; a
  server-stream lets the Events page render incrementally and (later) live-tail.
- **`GetLineage`** — for deep/large graphs, streaming nodes as the traversal
  discovers them lets the `@xyflow/react` canvas paint progressively instead of
  blocking on the whole graph.
- **`Search`** — server-stream search-as-you-type results.

Other ConnectRPC wins that REST can't give us, independent of streaming: binary
protobuf framing, generated TS clients with typed errors, and bidi for future
interactive features. **Recommendation:** stay unary for v1; revisit streaming
once the UI is in place and there's a measured latency/payload problem to solve.

## Consequences

- **One source of truth for the shape.** `proto/headwaters/read/v1` is the
  canonical definition the hand-written server, the future generated clients, and
  the UI all reference. `crates/headwaters/src/read/model.rs` and the proto
  are kept in lockstep (the proto matches the serde structs field-for-field,
  including `currentVersion` and the tag/stats messages the old proto omitted).
- **Clean spec/non-spec boundary.** `lineage.v1` is now purely OpenLineage-aligned
  (event/facet model + `IngestService`); the read surface is unambiguously
  headwaters-owned in `headwaters.read.v1`.
- **No new runtime deps or generated serving code** enter the build this phase —
  lower risk, and the Marquez wire contract is preserved exactly because the
  hand-written server is unchanged.
- **A deferred follow-up exists**: wiring generated serving + clients depends on
  the four Trestle improvements above (or on accepting a documented wire-contract
  change, since these APIs aren't a published spec). Tooling reference: invoke
  `trestle` from the local `../trestle` checkout (`cargo run --manifest-path
  ../trestle/crates/trestle/Cargo.toml --bin trestle -- generate …`) and git-pin
  `olai-*` deps to a rev — the published crates are not yet usable.
