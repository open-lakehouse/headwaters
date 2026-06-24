# 0008 — Tags are discovered facts carried as OpenLineage events

> Status: **Accepted** (2026-06). Implemented in
> `crates/lineage-service/src/projection/processors/tags.rs` (the tags
> processor) and the `tags` / `tag_assignments` projection tables
> (`migrations/0003_facet_model.sql`). Builds on the projection model in ADR
> [0007](0007-mutation-ir-projection-pipeline.md); the propagation that consumes
> these tags is ADR [0009](0009-tag-pii-propagation.md).

## Context

We want tags on datasets, fields, and jobs — both the ones producers attach on
normal lineage events (the OpenLineage `tags` facet, and per-field `schema.tags`)
and, more importantly, **tags asserted by a separate system that *discovers* a
fact** — e.g. a PII scanner finding that `raw.users.email` contains PII and
needing to annotate that column so policy can act on it ("where does this PII
land downstream?", ADR 0009).

The question was how a discovery like that enters the system. Two options:

1. A REST write endpoint (`PUT /tags/...`) that mutates the `tag_assignments`
   projection directly.
2. The discovery is emitted as an **OpenLineage event** (a `DatasetEvent`
   carrying a `tags` / `schema.tags` facet), appended to the `events` log and
   projected like any other event.

Option 1 breaks the load-bearing invariant of the hybrid-CQRS design (ADR 0006):
the read model is a pure projection of the event log, rebuildable by replay. A
directly-mutated `tag_assignments` would be wiped by `projection::rebuild` unless
carved out of the rebuild — splitting the source of truth.

## Decision

**Tags are facts carried on events.** A system that discovers a tag-worthy fact
emits an OpenLineage event whose facets express it:

- whole-dataset / whole-job tags via the `tags` facet;
- field-level tags via the `schema` facet's per-field `tags`.

The `TagsProcessor` parses these into `UpsertTag` + `TagAssignment` mutations
(targeting `Dataset` / `DatasetField` / `Job`), exactly like any other facet
processor. Because the assignment lives in the event log, it is **rebuildable**:
truncate + replay reproduces it, and the projection stays the single source of
truth. A discovery event is an ordinary `DatasetEvent` — no custom event type,
no new ingest path — so any aware producer can emit one today, and a future REST
"tag this" endpoint would simply **append a synthetic event** rather than write
the projection.

## Consequences

- **The rebuild-from-log invariant holds for tags.** No special-casing in
  `projection::rebuild`; tag assignments are folded by replay like everything
  else. Proven by a rebuild acceptance test.
- **Discovery is decoupled and pluggable.** A scanner, a classifier, or a human
  tool all annotate by emitting events; the service needs no per-source code.
  This is the seam we may later formalize into a first-class **fact /
  annotation event** vocabulary (a typed "assertion about an entity") — noted
  now, deliberately not formalized, to avoid over-fitting before a second
  producer shape exists.
- **Tags are monotonic / add-only** (latest-wins on `assigned_at`), matching how
  Marquez treats facet-derived tags. Tag *removal* is not modeled yet; when
  needed it would be another fact event (a "tag removed" assertion), preserving
  the same invariant.
- **Revisit trigger:** if discovery sources need richer assertions (confidence,
  evidence, who/when beyond the event envelope), formalize the fact-event type
  then, rather than overloading `tags`.
