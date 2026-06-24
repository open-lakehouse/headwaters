# Marquez compatibility & divergences

Where `lineage-service` **matches**, **intentionally differs from**, and **goes
beyond** the OpenLineage reference implementation, [Marquez].

This is a living document: it is updated as each facet-processing phase adds
interpretation. Claims marked **[conformance]** are backed by an assertion in
`crates/lineage-service/tests/conformance_test.rs`, which posts identical events
to a real Marquez and to our service and diffs their read APIs on a normalized
semantic subset. See ADR [0015](adr/0015-hybrid-cqrs-postgres-storage.md)
(storage) and [0016](adr/0016-mutation-ir-projection-pipeline.md) (facet
pipeline).

[Marquez]: https://github.com/MarquezProject/marquez

## Proven equivalent [conformance]

For the same OpenLineage events, our read APIs reconstruct the same:

- **Table-level lineage graph** — same nodes and edges.
- **Column-level lineage** — same input-field → output-field dependency
  pairings. (Compared orientation-insensitively: Marquez orients column-lineage
  edges output→input, while our graph — and Marquez's *table-level* graph —
  orient input→output. The pairing is identical.)
- **Run state + job/dataset model** — same run state, inputs/outputs, schema
  fields.
- **Facet round-trip** — the `nominalTime` run facet is preserved verbatim by
  both under `run.facets.nominalTime`.

## Intentional divergences (design choices)

| Area | Marquez | Us | Why |
|---|---|---|---|
| Consistency | Normalizes **synchronously** in the ingest request (read-your-writes) | **Async projection** off an append-only log; reads eventually consistent | Spike-tolerant ingest + replayable projections. ADR 0015. |
| Facets | *Interprets* facets opinionatedly during ingest (see table below) | **Opaque pass-through** by default; interpret a facet only when a read derives value from it | New/custom facets cost nothing on ingest; we add interpretation deliberately, per phase. ADR 0016. |
| `nominalTime` | Hoists to top-level `run.nominalStartTime`/`End` (only for a `_schemaURL` it recognizes) and parses into `run.args` truncated to minutes | Keep the facet verbatim in the run-facets blob | Marquez's hoisting is internal and inconsistent; the verbatim facet is the stable compatibility surface. |
| Column-edge orientation | output→input | input→output (consistent with table-level) | One internal convention. Normalized away in the conformance diff. |

## Stubbed / not-yet-implemented endpoints

- `/api/v1/tags`, `/api/v1/stats/*` — currently empty-but-200 stubs (the UI
  renders empty rather than 404ing). Real implementations land in the tags +
  stats phase.
- Marquez's deprecated CRUD / run-state **write** endpoints (PUT
  namespace/dataset/job, POST `runs/{id}/start|complete|fail`, …) — **not
  implemented**; the event model replaces them and the web UI does not call
  them.

## Per-facet interpretation status

How Marquez interprets each well-known facet (beyond storing JSON), and where we
stand. "Interpreted" = promoted into the relational model, not just retained.

| Facet | Marquez interprets into | Our status |
|---|---|---|
| `schema` | one `dataset_fields` row per column | dataset fields cache (per-column rows: Phase 1) |
| `columnLineage` | `column_lineage` edge table (output datasets) | column-lineage graph from the lifted facet (edge table: Phase 1) |
| `documentation` | `description` on job/dataset | job description ✓ (dataset: Phase 3) |
| `tags` | tag tables (via REST, not ingest) | job tags ✓ (catalog + assignments + propagation: Phase 4) |
| `dataSource` | a `sources` row (name + connection_url) | **pass-through** (Phase 3) |
| `sourceCodeLocation` | job `location` | **pass-through** (Phase 3) |
| `parent` (ParentRunFacet) | creates parent run/job rows, links them | **pass-through** (Phase 3) |
| `nominalTime` | run nominal_* columns + args | **pass-through** (verbatim facet) |
| `lifecycleStateChange` | `DROP` soft-deletes the dataset | **pass-through** (Phase 3) |
| `ownership` | not interpreted on ingest | **pass-through** |
| `sql`, `errorMessage` | not interpreted (stored as facet rows) | **pass-through** |

## Capabilities beyond Marquez

- **Tag / PII propagation** (Phase 4): "where does data tagged `pii` land
  downstream?" — a transitive closure over column (then table) lineage seeded
  from tag assignments. Marquez has no equivalent API. ADR 0018.
- **Tags as discovered facts** (Phase 4): tag/annotation writes are modeled as
  appended events (a system that *discovers* PII raises a tag event), keeping
  everything rebuildable from the log. ADR 0017.
