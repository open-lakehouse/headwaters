# Architecture Decision Records

Design decisions for the lineage crates, in [MADR-lite](https://adr.github.io/madr/)
format (Title, Status, Context, Decision, Consequences).

These were extracted alongside the code from
[`open-lakehouse`](https://github.com/open-lakehouse); the original numbering is
preserved, so the sequence is intentionally sparse (non-lineage ADRs stayed in
the source repo). A few records cross-reference decisions that live there
(e.g. per-session credential isolation, per-query governance context) — those
links point back to `open-lakehouse`.

| ADR | Decision |
|---|---|
| [0003](0003-per-statement-run-id-correlation.md) | Per-statement `run_id` correlation |
| [0009](0009-lineage-service-unity-catalog-write-path.md) | lineage-service Unity Catalog write path & credential vending |
| [0012](0012-client-forwarded-lineage-metadata.md) | Client-forwarded lineage metadata (gRPC headers) |
| [0013](0013-column-level-lineage-positional-resolution.md) | Column-level lineage via positional resolution |
| [0014](0014-openlineage-planner-vs-rule.md) | OpenLineage planner vs rule (plan-carried marker) |
| [0015](0015-hybrid-cqrs-postgres-storage.md) | Hybrid-CQRS Postgres storage (raw event log + async projection) |
| [0016](0016-mutation-ir-projection-pipeline.md) | Facet processing as a backend-agnostic Mutation IR |

See also [`docs/marquez-compatibility.md`](../marquez-compatibility.md) — a
living reference for where the service matches, diverges from, and goes beyond
the Marquez reference implementation.
