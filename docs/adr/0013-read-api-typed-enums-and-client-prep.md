# 0013 — Read-API enums, additive filters, and consumer-focused docs

> Status: **Accepted** (2026-06). Refines the `headwaters.read.v1` proto from ADR
> [0010](0010-read-api-proto-source-of-truth.md) ahead of generating a Rust read
> client (and CLI). Touches `proto/headwaters/read/v1/{read,service}.proto` and
> the hand-written server in `crates/headwaters/src/read/`.

## Context

We are about to generate a publishable Rust client (and a CLI) from
`headwaters.read.v1`. ADR 0010 made the proto the source of truth for the read
API's shape; since regenerating the server and clients touches every generated
artifact at once, this was the moment to improve the proto's quality before
clients depend on it. The constraint from 0010 still holds: the read API is
**Marquez wire-compatible** — camelCase JSON, the `type` / `inEdges` / `outEdges`
keys, `?nodeId=` query params, opaque facet bags — and the `conformance-it`
test against the Marquez reference is the guardrail.

Three things were worth fixing: stringly-typed `type` / `state` fields (no
exhaustive matching for clients), no server-side `Search` filtering (clients
would post-filter), and doc comments written for maintainers (server internals)
rather than for API consumers (the comments flow into the generated Rust/TS
rustdoc).

## Decision

**1. Model `type` / `state` as proto enums.** `JobType`, `DatasetType`,
`RunState`, and `EntityKind` (used by `SearchResult.type` and `LineageNode.type`)
replace the `string` fields. This is **wire-compatible**: buffa's `json=true`
serializes an enum by its proto value name, so `BATCH` / `COMPLETED` / `DATASET`
go out exactly as before — verified by a codegen spike and by `conformance-it`
staying green. The hand-written server maps the stored run-state string to
`RunState` via `from_proto_name`; an unrecognized value round-trips as
`EnumValue::Unknown` rather than failing.

Two consequences of the enum encoding worth recording:

- **Enum values are bare** (`BATCH`, not `JOB_TYPE_BATCH`). The value name *is*
  the wire string, so the `ENUM_VALUE_PREFIX` buf-lint rule is excepted in
  `buf.yaml`. Prefixing would change the JSON contract.
- **A zero value field is omitted from JSON** (`skip_serializing_if =
  is_default_enum_value`). Every enum has a `*_UNSPECIFIED = 0` zero value, and
  the server always sets a concrete non-zero value, so `type` / `state` stay
  present on the wire exactly as before — but the server must never emit a
  zero-value `type`/`state` (the Marquez `marquez_compat` layer keys off `type`
  being present).

**2. Add additive `Search` filters.** `SearchRequest` gains an optional `type`
(`EntityKind`) and `namespace`, applied in SQL. Omitting them preserves the prior
all-kinds / all-namespaces behavior, so it is non-breaking.

**3. Rewrite doc comments for consumers.** The `read.proto` / `service.proto`
comments now describe what each message/field/RPC *is* and its value/format,
not how the server computes it. Server-internal rationale (projector mechanics,
byte-compat history, codegen notes) is dropped or left to these ADRs. Timestamps
are documented uniformly as RFC 3339 strings; the nodeId grammar, the
`limit`/`offset`/`total_count` pagination contract, and the `depth` default/cap
are stated where a caller meets them.

**4. Timestamps stay `string`; high-value facet lifting is deferred.** Migrating
`*_at` / `*_time` to `google.protobuf.Timestamp` would render identically on the
wire but ripple through the server's formatting helpers for no consumer-visible
gain, so they stay documented RFC 3339 strings (the client centralizes parsing).
Lifting the high-value facets (schema, columnLineage, SQL, errorMessage) out of
the opaque `Struct` bags into typed sub-messages — as `lineage.v1` did for
`ColumnLineageDatasetFacet` — is **deferred to phase 2**: it cannot be done
without reworking how the server builds `facets`, and the upcoming CLI can
interpret the known facet keys in its formatter layer in the meantime.

## Consequences

- Generated Rust/TS clients get exhaustive enum matching instead of magic
  strings, and rustdoc that reads as API documentation.
- The Marquez wire contract is unchanged: enums serialize to the same strings,
  the new `Search` fields are additive, and `conformance-it` + the `postgres-it`
  camelCase/wire tests stay green.
- One implicit invariant is now load-bearing: the server must always set a
  concrete (non-zero) `type` / `state`. This is true today and asserted by the
  read integration tests.
- A phase-2 follow-up remains: lift the high-value facets into typed read-proto
  fields so clients don't re-parse opaque JSON.
