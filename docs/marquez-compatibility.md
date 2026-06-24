# Marquez compatibility & divergences

Where `lineage-service` **matches**, **intentionally differs from**, and **goes
beyond** the OpenLineage reference implementation, [Marquez].

This is a living document: it is updated as each facet-processing phase adds
interpretation. Claims marked **[conformance]** are backed by an assertion in
`crates/lineage-service/tests/conformance_test.rs`, which posts identical events
to a real Marquez and to our service and diffs their read APIs on a normalized
semantic subset. See ADR [0006](adr/0006-hybrid-cqrs-postgres-storage.md)
(storage) and [0007](adr/0007-mutation-ir-projection-pipeline.md) (facet
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
| Consistency | Normalizes **synchronously** in the ingest request (read-your-writes) | **Async projection** off an append-only log; reads eventually consistent | Spike-tolerant ingest + replayable projections. ADR 0006. |
| Facets | *Interprets* facets opinionatedly during ingest (see table below) | **Opaque pass-through** by default; interpret a facet only when a read derives value from it | New/custom facets cost nothing on ingest; we add interpretation deliberately, per phase. ADR 0007. |
| `nominalTime` | Hoists to top-level `run.nominalStartTime`/`End` (only for a `_schemaURL` it recognizes) and parses into `run.args` truncated to minutes | Keep the facet verbatim in the run-facets blob | Marquez's hoisting is internal and inconsistent; the verbatim facet is the stable compatibility surface. |
| Column-edge orientation | output→input | input→output (consistent with table-level) | One internal convention. Normalized away in the conformance diff. |
| Dataset versions | A version per run, keyed to the creating run | A version per **distinct schema snapshot** (deterministic UUIDv5 of the fields), keyed to the producing run; `/versions` returns the real schema history (empty until a `schema` facet is seen, vs. our earlier fabricated single version) | Schema-change-driven versioning is the useful axis ("how did this dataset evolve?") and keeps replay idempotent. |

## Stubbed / not-yet-implemented endpoints

- `/api/v1/tags` and `/api/v1/stats/*` are now **real** (tag catalog from the
  projection; time-bucketed event/asset counts off the log). No longer stubs.
- Marquez's deprecated CRUD / run-state **write** endpoints (PUT
  namespace/dataset/job, POST `runs/{id}/start|complete|fail`, …) — **not
  implemented**; the event model replaces them and the web UI does not call
  them.

## Per-facet interpretation status

How Marquez interprets each well-known facet (beyond storing JSON), and where we
stand. "Interpreted" = promoted into the relational model, not just retained.

| Facet | Marquez interprets into | Our status |
|---|---|---|
| `schema` | one `dataset_fields` row per column | ✓ per-column `dataset_fields` rows (+ the `datasets.fields` cache) |
| `columnLineage` | `column_lineage` edge table (output datasets) | ✓ `column_lineage_edges` table; per-output-field latest-wins |
| `documentation` | `description` on job/dataset | ✓ job + dataset `description` |
| `tags` (+ field `schema.tags`) | tag tables (via REST, not ingest) | ✓ `tags` catalog + `tag_assignments` (dataset/field/job), from ingest **and** synthetic fact events (ADR 0008); drives PII propagation (ADR 0009) |
| `dataSource` | a `sources` row (name + connection_url) | ✓ `sources` row + dataset `source_name` |
| `sourceCodeLocation` | job `location` | ✓ job `location` |
| `parent` (ParentRunFacet) | creates parent run/job rows, links them | ✓ run `parent_run_id` + job `parent_namespace`/`parent_name` (we link, but do not synthesize standalone parent run/job rows) |
| `nominalTime` | run nominal_* columns + args | ✓ run `nominal_start`/`nominal_end` (we also keep the verbatim facet) |
| `lifecycleStateChange` | `DROP` soft-deletes the dataset | ✓ `DROP` sets dataset `deleted` |
| `ownership` | not interpreted on ingest | **pass-through** |
| `errorMessage` | not interpreted (stored as facet rows) | ✓ run `error_message` (a step beyond Marquez, which derives failure from `eventType`) |
| `sql` | not interpreted (stored as facet rows) | **pass-through** |

## Capabilities beyond Marquez

- **Tag / PII propagation** — `GET /api/v1/tags/{tag}/downstream`: "where does
  data tagged `pii` land downstream?" — a transitive closure over column lineage
  seeded from tag assignments. Marquez has no equivalent API. ADR 0009.
- **Tags as discovered facts** — tag/annotation writes are modeled as appended
  OpenLineage events (a system that *discovers* PII raises a tag event), keeping
  everything rebuildable from the log. ADR 0008.
