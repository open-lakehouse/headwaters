# 0009 — Tag / PII propagation as a query-time graph traversal

> Status: **Accepted** (2026-06). Implemented in
> `crates/headwaters/src/read/queries.rs`
> (`LineageStore::tag_downstream`) and exposed at
> `GET /api/v1/tags/{tag}/downstream` (`read/http.rs`). Consumes the tags from
> ADR [0008](0008-tags-as-discovered-facts.md) and the `column_lineage_edges`
> projection from ADR [0007](0007-mutation-ir-projection-pipeline.md).

## Context

The motivating use case for the richer model is **policy reasoning over
lineage**: given a tag (canonically `pii`), answer *"where does tagged data land
downstream through our processing graph?"* — so that a column tagged PII at its
source is known to also require protection everywhere it flows. This is a
reachability query over the column-lineage graph, seeded from the current tag
assignments.

The choice was whether to **materialize** a tag-reachability table (maintained
by the projector) or compute the closure **at query time**.

## Decision

Compute it at **query time**, with a `WITH RECURSIVE` transitive closure over
`column_lineage_edges` (field granularity), seeded from `tag_assignments`:
directly-tagged fields, plus every field of a tagged dataset. Bounded by the
same `MAX_DEPTH` the lineage graph query uses. `pii` is just a conventional tag
name — nothing is special-cased; the endpoint works for any tag.

Query-time over materialized because:

- **Correctness for free.** The closure always reflects the current projection;
  there is no second derived table to keep replay-consistent. Materializing
  reachability would double the idempotency/rebuild surface (ADR 0006's
  invariant) for a read pattern that is not yet hot.
- **The graph is the cost, and it's already indexed.** `column_lineage_edges`
  has in/out indexes; recursive traversal over it is the same shape as the
  table-level lineage query we already serve. Tag assignments are sparse.
- **Marquez itself traverses lineage at query time** — consistent with the
  reference's performance posture.

A table-level fallback (over `lineage_edges`) covers datasets that lack column
lineage; field-level is preferred where present.

## Consequences

- **A capability beyond Marquez.** Marquez has no tag-propagation API; this is
  net-new ("where does PII land downstream?") and is recorded as a
  beyond-Marquez capability in `docs/marquez-compatibility.md`.
- **No new projection state**, so no new rebuild path — the propagation is a
  pure read over existing projections. Proven by an acceptance test: a scanner's
  synthetic PII tag event (ADR 0008) flows through column lineage to the
  downstream field, and survives a rebuild.
- **Depth-capped**, like the lineage graph, to bound dense graphs; the cap is a
  known limit (very long transformation chains truncate) rather than silent.
- **Revisit trigger:** if propagation becomes a hot path (interactive
  policy-enforcement at scale), materialize a reachability projection — folded
  by the projector and added to the rebuild set — and serve reads from it. The
  query-time version stays the correctness oracle.
