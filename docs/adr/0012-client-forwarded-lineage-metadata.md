# 0012 — Client-forwarded lineage metadata over gRPC headers

> Status: **Accepted** (2026-06). Implemented in `crates/hydrofoil/src/lineage.rs`
> (header parsing), `crates/hydrofoil/src/server.rs` (request-path wiring), and
> `crates/lineage-service/src/read/queries.rs` (read-side surfacing). Refines
> [`docs/open-lineage-design.md`](../open-lineage-design.md); builds on
> [ADR 0003](0003-per-statement-run-id-correlation.md) (run identity) and
> [ADR 0005](0005-per-query-agent-governance-context.md) (agent context).

## Context

Hydrofoil's OpenLineage events were structurally correct but metadata-poor: a
job carried a name and the SQL facet; a run carried parent + processing-engine
facets. Nothing recorded *who* ran a query, *on whose behalf / why*, or any
business context (tags, owners, description) — the things that make a lineage
graph navigable beyond its edges.

The OpenLineage **Spark integration** is the prior art for how clients supply
this: `spark.openlineage.namespace`, `appName`, `parent*`, `job.tags`,
`job.owners.<type>`, all session-level configuration that the listener folds
into emitted events. Hydrofoil's analog of "session configuration a client
controls" is **gRPC request metadata**, already used for the principal
(`x-hydrofoil-*`, ADR 0008), agent context (ADR 0005), and parent-run lineage
context.

## Decision

Extend the `x-openlineage-*` header family (parsed in
`crates/hydrofoil/src/lineage.rs`) to Spark parity, and fold the existing
governance context into a custom run facet:

| header | target | grammar |
|---|---|---|
| `x-openlineage-job-namespace` | job namespace | plain string |
| `x-openlineage-job-name` | job name (ADR 0003 / S11) | plain string |
| `x-openlineage-job-description` | `documentation` job facet | free text |
| `x-openlineage-job-tags` | `tags` job facet | `key[:value[:source]]`, `;`-separated |
| `x-openlineage-job-owners` | `ownership` job facet | `type:name`, `;`-separated |
| `x-openlineage-parent-*` (+root) | `parent` run facet | (pre-existing) |

- **Namespace is header-overridable with the configured namespace as default.**
  This *revises* the earlier stance (S11 / the C7 fix) that the namespace was
  config-driven only: a client may scope its jobs (e.g. one namespace per
  pipeline), and the server's `lineage.namespace` config remains the fallback
  when the header is absent. Dataset naming keeps using the configured
  namespace — datasets are storage-scoped, not request-scoped.
- **Malformed metadata never fails a query.** Entries that don't parse are
  skipped; lineage is observability, not admission control.
- **Governance provenance becomes a custom `hydrofoil` run facet**
  (`lineage::hydrofoil_run_facet`): the resolved principal (when not
  `User::"anonymous"`) and any `x-hydrofoil-agent-*` context (id, session,
  task, purpose). It is attached at planning and refreshed per execution when
  the executing request carries its own agent context (one prepared handle may
  serve many agent tasks). No facet is emitted for an anonymous, agent-less
  request — the facet marks known provenance, not noise.
- **Transport**: facets flow through the existing `LineageContext`
  `job_facets`/`run_facets` extras maps (serde-flattened into the event), so
  the `datafusion-open-lineage` builder needed no changes; the typed facet
  structs (`DocumentationJobFacet`, `OwnershipJobFacet`, `TagsJobFacet`) live
  in `crates/open-lineage/src/facets.rs` for spec-correct shapes.
- **Read side**: the Marquez-compatible API surfaces `description` and `tags`
  on jobs (folded latest-event-wins from `raw_json`, mirroring the `edges_at`
  pattern); the run-facets endpoint already returned all facets, including
  `hydrofoil`, unchanged.

Clients pass headers via the ADBC Flight SQL option prefix
`adbc.flight.sql.rpc.call_header.<header>` (database- or connection-level, so
per-query context can change between statements on one connection). See
`notebooks/lineage_metadata.py` for the reference client.

## Consequences

- Lineage events answer who/what/why: principal + agent in the `hydrofoil` run
  facet, business context in spec job facets — visible in the Marquez UI (job
  description/tags) and via `GET /api/v1/jobs/runs/{id}/facets`.
- The header grammar is intentionally Spark-shaped, so orchestrators already
  emitting OpenLineage Spark properties map 1:1 onto hydrofoil headers.
- A generic raw-JSON facet passthrough header was **rejected**: unvalidated
  client JSON inside events is an injection surface; new facets get explicit
  headers + parsers instead.
- Headers are client-asserted. The principal in the `hydrofoil` facet is the
  *resolved* session principal (server-side truth), but tags/owners/description
  are taken on faith — acceptable for lineage annotation, not for policy input.
