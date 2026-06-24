# 0015 — Hybrid-CQRS Postgres storage: raw event log + async projection

> Status: **Accepted** (2026-06). Implemented in
> `crates/lineage-service/src/writer/` (ingest → `events`), `src/projection/`
> (the async projector), `src/read/` (the read store), and
> `migrations/0001_init.sql` + `0002_uuidv7_and_audit.sql`. Supersedes the
> earlier Delta-Lake/Unity storage for `lineage-service`. See also
> [`docs/marquez-compatibility.md`](../marquez-compatibility.md).

## Context

`lineage-service` ingests OpenLineage events and serves a Marquez-compatible
read API. The lineage UI needs a *materialized* model (namespaces, jobs,
datasets, runs, a lineage graph), but the ingest side only ever receives raw
events. We had to choose how to store and serve them.

The reference implementation (Marquez, Postgres) keeps **both** a raw
append-only `lineage_events` (jsonb) table **and** a normalized model, and
normalizes **synchronously inside the ingest transaction** — which is its
documented throughput bottleneck (single-event ingest, costly facet queries
against the raw table). We wanted spike-tolerant ingest and the freedom to
re-derive projections we didn't design up front.

## Decision

Adopt **hybrid CQRS on a single Postgres**:

1. **Source of truth: an append-only `events` table.** Ingest parses/classifies
   an event (buffa proto), the `BufferedWriter` batches, and the `PostgresSink`
   bulk-inserts rows. A `BIGSERIAL seq` orders the log and is the projection
   cursor. Ingest returns `202` without blocking on normalization.
2. **Async projection.** A background `Projector` polls `events WHERE seq >
   last_seq`, folds each into the normalized read tables, and advances a
   `projection_state` cursor — all in one transaction per batch. Reads are
   therefore **eventually consistent** (≤ one poll interval behind ingest),
   which a lineage browse/analysis UI tolerates.
3. **Idempotent, replayable fold.** Every projection write is an event-time-
   guarded `ON CONFLICT` upsert (latest-wins for edges/metadata/schema; terminal
   run states never downgraded). So the fold is order-insensitive, and
   `projection::rebuild` (truncate read tables + reset cursor + re-fold the log)
   reproduces identical tables — letting us re-derive new projections by replay.
4. **Read model + graph.** `LineageStore` serves the Marquez contract with
   indexed `sqlx` queries; the lineage graph is a `WITH RECURSIVE` walk over a
   projected `lineage_edges` table (Marquez's approach). DB-side `uuidv7()` +
   `trigger_updated_at()` (the unitycatalog-rs conventions) supply surrogate ids
   and audit timestamps so we expose the same valuable identity/version fields
   Marquez does.

Postgres replaces Delta/Unity entirely for this service.

## Consequences

- **Ingest is decoupled from normalization cost** — load spikes hit a cheap
  append, not a fan-out of upserts. This is the deliberate divergence from
  Marquez's synchronous model; the price is eventual-consistency on reads.
- **Replayability is a first-class property.** Because the read model is a pure
  projection of the log, we can change the projection logic and rebuild — which
  is exactly what the extensible facet pipeline (ADR
  [0016](0016-mutation-ir-projection-pipeline.md)) and future facet processors
  rely on.
- **A differential conformance test** (`tests/conformance_test.rs`) proves the
  reconstructed lineage matches Marquez for identical events.
- **Revisit trigger:** if read latency from the per-request `WITH RECURSIVE` /
  fold-free reads becomes hot, consider materialized projections or read
  replicas; if strong read-your-writes is ever required, a synchronous
  projection path for specific endpoints could be added without changing the
  log.
