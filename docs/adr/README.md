# Architecture Decision Records

Design decisions for the Headwaters crates, in [MADR-lite](https://adr.github.io/madr/)
format (Title, Status, Context, Decision, Consequences).

These were extracted from [`open-lakehouse`](https://github.com/open-lakehouse)
and renumbered contiguously for this independent repo. Some records build on
decisions that remain in the upstream host service (e.g. per-session credential
isolation, per-query agent-governance context, principal-resolution headers);
those are referenced inline as prose rather than as numbered ADRs here.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-per-statement-run-id-correlation.md) | Per-statement `run_id` correlation | Accepted |
| [0002](0002-lineage-service-unity-catalog-write-path.md) | lineage-service Unity Catalog write path & credential vending | Superseded by 0006 |
| [0003](0003-client-forwarded-lineage-metadata.md) | Client-forwarded lineage metadata (gRPC headers) | Accepted |
| [0004](0004-column-level-lineage-positional-resolution.md) | Column-level lineage via positional resolution | Accepted |
| [0005](0005-openlineage-planner-vs-rule.md) | OpenLineage planner vs rule (plan-carried marker) | Accepted |
| [0006](0006-hybrid-cqrs-postgres-storage.md) | Hybrid-CQRS Postgres storage (raw event log + async projection) | Accepted |
| [0007](0007-mutation-ir-projection-pipeline.md) | Facet processing as a backend-agnostic Mutation IR | Accepted |
| [0008](0008-tags-as-discovered-facts.md) | Tags are discovered facts carried as OpenLineage events | Accepted |
| [0009](0009-tag-pii-propagation.md) | Tag / PII propagation as a query-time graph traversal | Accepted |
| [0010](0010-read-api-proto-source-of-truth.md) | Read API modeled in protobuf (`headwaters.read.v1`); REST stays hand-written for now | Accepted |

See also [`docs/marquez-compatibility.md`](../marquez-compatibility.md) — a
living reference for where the service matches, diverges from, and goes beyond
the Marquez reference implementation.
