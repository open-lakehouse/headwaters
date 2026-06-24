# 0016 — Facet processing as a backend-agnostic Mutation IR

> Status: **Accepted** (2026-06). Implemented in
> `crates/lineage-service/src/projection/`: `mutation.rs` (the `Mutation` IR),
> `processor.rs` + `processors/` (pure `FacetProcessor`s), `registry.rs`
> (composition), `applier.rs` + `backend/postgres.rs` (the `PgApplier`).
> Builds on the storage model in ADR
> [0015](0015-hybrid-cqrs-postgres-storage.md).

## Context

The original projection (`projection/apply.rs::apply_event`) folded a raw event
into the read model in one function that **entangled parsing with
Postgres-specific SQL** — `parse_refs`/`parse_job_meta`/`parse_output_schemas`
called inline, interleaved with `INSERT ... ON CONFLICT` statements. Two
problems followed as we set out to interpret more facets (schema fields, column
lineage, sources, dataset versions, parent runs, tags) and to support storage
backends beyond Postgres:

1. **Parse logic would be replicated per backend.** A second backend would
   re-implement the facet parsing, not just the writes.
2. **Every projection test needs a live Postgres**, because there is no layer to
   assert on between "raw event" and "rows written."
3. **Idempotency is re-expressed in every statement**, so a new facet's SQL
   could silently break replay-safety.

The buffa-generated facet types deserialize robustly (camelCase rename +
snake_case alias on every field, `_producer`/`_schemaURL` handled), so parsing
should lean on their serde, not hand-rolled JSON walking.

## Decision

Split the projection into three layers with a **backend-agnostic command IR**
between them:

1. **`Mutation` (IR).** An enum describing one change to the read model
   (`UpsertJob`, `UpsertRunState`, `UpsertDataset`, `UpsertLineageEdge`, …),
   each state-bearing variant carrying the event time `at`.
2. **`FacetProcessor` (parse, pure).** `fn process(&self, ev: &RawEvent, out:
   &mut Vec<Mutation>)` — synchronous, no I/O, output is a function of *this
   event only*. One processor per concern, composed by a `ProcessorRegistry`
   (`with_well_known()` for built-ins; `register(...)` for custom/future facet
   processors). Adding a facet is a new processor + one register line, never an
   edit to a central match.
3. **`MutationApplier` (apply, backend-specific).** Translates each `Mutation`
   to writes. The `PgApplier` holds the existing `ON CONFLICT` SQL, verbatim,
   one match arm per variant. It is the **single canonical place** the
   event-time / terminal-rank idempotency guards live.

We chose the Mutation IR over the alternative — giving processors a `&mut dyn
ProjectionBackend` with semantic methods (`upsert_dataset_field`, …) — because:

- It directly serves "parse once regardless of backend": a new backend
  implements one applier trait and **never re-parses**.
- Processors become **pure, DB-free unit-testable** (`RawEvent -> Vec<Mutation>`),
  the fast-feedback layer the entangled fold couldn't offer.
- The write vocabulary lives in one enum; the backend trait's method set doesn't
  grow per write shape.
- Idempotency lives once, in the applier — a new processor can't break replay.

The transaction stays concrete inside `PgApplier` (`apply(&mut Transaction,
&Mutation)`), matching the pragmatic `EventSink` style rather than abstracting it
behind a GAT — the IR is the load-bearing seam, not the transaction plumbing.

## Consequences

- **Phase 0 was a behavior-preserving refactor**: the existing SQL moved into
  `PgApplier` arms unchanged, and the full Postgres acceptance + Marquez
  conformance suites passed unchanged, with new DB-free processor unit tests
  added on top.
- **Richer facet interpretation is now additive** — each new well-known or
  custom facet is a localized processor (+ a `Mutation` variant + a `PgApplier`
  arm only if it needs a new write shape).
- **A second backend is one `MutationApplier` impl** (e.g. an in-memory applier
  as a parity proof), with zero parse-logic duplication.
- The idempotency contract (pure processors; every state-bearing mutation
  carries `at`) is documented on `FacetProcessor` and enforced by the applier,
  keeping `projection::rebuild` correct as processors are added.
- **Revisit trigger:** if a backend genuinely cannot express a `Mutation`
  variant's guard as an upsert, or if cross-mutation transactional ordering
  becomes load-bearing, reconsider the flat per-mutation apply.
