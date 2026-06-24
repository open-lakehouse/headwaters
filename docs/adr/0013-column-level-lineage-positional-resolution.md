# 0013 — Column-level lineage via positional plan resolution

> Status: **Accepted** (2026-06). Implemented in
> `crates/open-lineage/src/column.rs` (resolver), `src/extract.rs` /
> `src/facets.rs` / `src/builder.rs` (facet emission), and
> `crates/lineage-service/src/read/queries.rs` (the `/api/v1/column-lineage`
> read view). Refines the "Column lineage" section of
> [`docs/open-lineage-design.md`](../open-lineage-design.md).

## Context

The original column-lineage extraction was removed in the S10 producer review
as unsound: it mapped column qualifiers to datasets *by name* (aliases/CTEs
fabricated datasets that don't exist; unqualified refs were silently dropped),
keyed a global map by bare output-column name with top-down visitation (deeper
same-named projections clobbered the root mapping; no transitive resolution),
and attached the facet to **input** datasets although the OpenLineage spec
defines `ColumnLineageDatasetFacet` on **outputs**, keyed by output field —
so consumers saw no column lineage at all. Events have carried table-level
lineage only since, with a design note sketching a sound replacement.

## Decision

Re-introduce column lineage as a **bottom-up, schema-position-indexed**
resolution over the optimized `LogicalPlan`, with these load-bearing choices:

1. **Positional indexing.** Every node's map has one entry per output-schema
   field; expression column refs resolve to child positions via
   `DFSchema::maybe_index_of_column`. Names are never used as keys, so
   scoping is exactly DataFusion's own: aliases/CTEs/self-joins cannot
   collide or fabricate datasets. Dataset identity is shared with the
   table-level extraction (one `dataset_for` helper), so the two walks can
   never disagree.
2. **Outputs only.** Only the root map is published, attached to the output
   dataset and keyed by the *target table's* field names (the SQL planner
   aligns a DML input positionally with the target schema). Pure SELECTs get
   no column lineage — the spec gives the facet no carrier without an output
   dataset, and a synthetic "query result" dataset would pollute the graph
   and break the no-inputs/no-outputs event suppression.
3. **Whole-facet degradation.** Any unhandled node, arity mismatch,
   unresolvable ref, or subquery-embedding expression drops the facet for the
   whole statement (with a `tracing::debug!` line). A partially-correct
   per-column facet is indistinguishable from a complete one, so partial
   emission would be dishonest; there is deliberately no name-based fallback.
   Table-level lineage is unaffected.
4. **Per-field indirect emission.** Statement-wide `INDIRECT` influences
   (filter/join/group/sort/window keys) are appended to every output field's
   `inputFields`, matching the Spark integration's default and what
   Marquez-style consumers render. The facet's dataset-level `dataset` array
   stays available as a future alternative.
5. **Taxonomy.** `DIRECT/IDENTITY` for bare column chains (preserved
   transitively through projections), `DIRECT/TRANSFORMATION` for any other
   expression, `DIRECT/AGGREGATION` for aggregate/window arguments; kinds
   max-merge along the plan. `masking` is always `false` — masking detection
   is explicitly out of scope.

On the consumer side, `GET /api/v1/column-lineage?nodeId=…` serves the
*latest* stored facet of the addressed output dataset as Marquez
`DATASET_FIELD` nodes (single-hop upstream; `depth`/`withDownstream` accepted
and ignored), built from the `column_lineage_json` column the writer already
persisted.

## Consequences

- Writes (`INSERT`/`UPDATE`/CTAS) carry spec-conformant column lineage;
  consumers (Marquez UI column view, the lineage notebooks) can render
  field-level graphs. Reads stay table-level by design.
- Known gaps, accepted and documented in the design doc: `COPY TO` (no
  lineage at all today), recursive CTEs (degrade — the work-table scan would
  fabricate a dataset), window-expression classification is coarse when the
  expression shape is unexpected (`TRANSFORMATION` for all refs — sound, just
  less precise).
- The facet adds payload to START/COMPLETE/FAIL events of writes (it rides
  the plan-time template); for wide tables this grows event size linearly
  with column count, which the bounded client queue already tolerates.
