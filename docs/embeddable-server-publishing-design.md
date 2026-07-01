# Embeddable `headwaters` server & crates.io publishing — design & handover

> Whether to publish the `headwaters` crate to crates.io as an **embeddable server
> surface** people can mount into their own axum server (without the UI), while it
> stays the standalone Docker service it is today; how far to make it pluggable;
> and how publishing lets us simplify the Docker-release trigger. This is a
> **design + handover document** — no code ships with it. Work is **deferred** in
> favor of other priorities (notably proving full end-to-end integration first).
> No ADR is filed yet — promote the core decisions to an ADR when implementation
> begins. Closely related: [CLI / server consolidation](cli-server-consolidation-design.md)
> (the `run`-extraction and publishing threads overlap — keep the two in step).

## Why

Today the `headwaters` crate (`crates/headwaters`) is `publish = false` /
`git_only = true`: a deployable service shipped only as a Docker image. Its Docker
release is triggered by a *manifest-version-derived git tag* because release-plz
omits `git_only` crates from its `releases` output (see the `release-plz.toml`
header and `.github/workflows/release-plz.yml`, the `headwaters-v<version>` →
`docker-release.yml` flow).

We want to offer `headwaters` as an **embeddable server surface**: something a
third party can `cargo add` and mount into their own axum server alongside their
own routes, with good Postgres defaults out of the box but the ability to bring
their own pools and their own event sink. It must keep working as the standalone
Docker service it is today. The UI stays **out** of the published crate
(Docker-only). Publishing also lets us retire the manifest-version workaround: a
real crates.io release emits a proper tag the Docker job can key off.

## Decisions

1. **Publish `headwaters` as a library + binary; do not split crates.** Keep one
   crate. Make the embedding surface a *deliberate, documented* public API rather
   than incidentally-`pub` modules. Prove the integration story incrementally
   (feature-data / experimenter path first).

2. **Make pluggable what people actually care about, and no more:**
   - **Bring-your-own pools** — separate read vs. write/projection Postgres pools.
     Already supported by the constructors; just needs documenting + an example.
   - **Bring-your-own sink** — tee events to a user's own store (lakehouse, Kafka,
     …) *alongside* the Postgres sink that keeps reads working. Requires the
     multi-sink fan-out fix below (§ Decision 3).
   - **Mount the API surface** into the host's own axum server.

3. **Fix the multi-sink fan-out so "bring your own sink" doesn't silently break
   reads.** Today a `debug_assert!(sinks.len() <= 1)` blocks the only safe shape
   (keep Postgres + add yours). A custom sink can therefore only *replace*
   Postgres, which makes the service **write-only with unreadable lineage** — the
   projector and read store only know how to read the Postgres `events` table.
   This is a contained, one-file fix and is the prerequisite for advertising the
   sink as a real extension point.

4. **Do NOT abstract the read store / projection source behind a trait yet.** The
   read API surface isn't proven end-to-end, so over-generalizing doesn't pay off,
   and it is a large refactor (`ReadService` is implemented directly on the
   concrete `LineageStore`, and the whole REST + Connect read surface threads that
   concrete type). Publish with **Postgres as the concrete, documented read +
   projection backend.** Revisit a read-backend trait once integration settles.

5. **No crate split now.** Extracting the backend-agnostic event/facet pipeline
   (converter, `FacetProcessor` registry, `Mutation` IR, sink/applier traits) into
   its own crate is worthwhile *eventually* but premature. Keep it in `headwaters`.

6. **Simplify the Docker release via a real crates.io release.** Once `headwaters`
   publishes, release-plz emits it in `releases` with a real version/tag; the
   Docker job keys off that and the manifest-version workaround is removed.

Rejected / deferred alternatives:

- **Abstract the read store now** — too much stable API to commit to before the
  read API is proven; large refactor through the whole read surface. Deferred.
- **Split out a pipeline/core crate now** — the genuinely reusable seam, but not
  worth the churn at this stage. Deferred.
- **Replace-the-sink as the pluggability story** — a foot-gun: it makes the
  service write-only. Superseded by the *additive* multi-sink fan-out.
- **Keep the manifest-version Docker trigger after publishing** — needless
  complexity once a real release tag exists.

## Enabling facts (verified 2026-06-29)

These make the chosen design cheap and shape its constraints:

- **The wiring is already embeddable — no global state.** The construction points
  are public functions taking injected dependencies:
  - `http::router(AppState, base_path) -> axum::Router` — a plain router to
    `.merge()` (`crates/headwaters/src/http.rs:74`).
  - `AppState { writer: BufferedWriterHandle, store: LineageStore }`, cloneable
    (`http.rs:33`).
  - `BufferedWriter::spawn(Vec<Arc<dyn EventSink>>, cfg)` — write side is already a
    trait (`EventSink`) (`writer/buffered.rs`, `writer/sink.rs`).
  - `Projector::spawn{,_with}(pool, interval[, extra])` — facet pipeline already
    pluggable via `FacetProcessor` (`projection/mod.rs`).
  - `LineageStore::new(pool)` (`read/mod.rs:72`), `Config::load(..)` with layered
    file/env config (`config.rs:173`).
- **The two halves communicate only through the Postgres `events` table, not a
  channel.** The in-process mpsc channel stops at the sink; the projector tails
  `events` by a `seq` (BIGSERIAL) cursor and folds into the read tables
  (`projection/mod.rs:121`). `PostgresSink` is "the only write path" into `events`
  (`writer/postgres.rs:5`). This is exactly why replacing the Postgres sink kills
  reads, and why the read side stays Postgres-concrete for now.
- **The multi-sink block is localized.** `append_all` re-sends the batch to *every*
  sink each retry and `flush` retries the whole batch on any failure
  (`writer/buffered.rs:262` / `:233`); a re-send to an already-succeeded sink would
  double-insert (the `events` INSERT is not idempotent). That double-insert risk is
  the sole reason for the `sinks.len() <= 1` guard (`buffered.rs:92`).
- **The UI is Docker-only already.** Served from `./web` on disk via `ServeDir`,
  404ing gracefully when absent (`http.rs`, `const UI_DIR = "web"`); the Dockerfile
  copies `node/app/dist` → `./web`. Nothing UI-related lives in the crate dir, so
  publishing the crate naturally excludes it.
- **The sibling published crates set the metadata template.** `openlineage-client`,
  `datafusion-openlineage`, `headwaters-proto`, `headwaters-client` already publish
  with `keywords` / `categories` / `documentation = "https://docs.rs/<crate>"`.

## Implementation plan (for the executing session)

### 1. Turn `headwaters` into a published library + binary
Files: `crates/headwaters/Cargo.toml`.

- Remove `publish = false` and the "never crates.io" comment block; add crates.io
  metadata matching the sibling published crates (`keywords`, `categories`,
  `documentation = "https://docs.rs/headwaters"`).
- Declare both targets explicitly: keep `[[bin]] name = "headwaters"` (point it at
  `src/main.rs`, or move to `src/bin/headwaters.rs`) and keep `src/lib.rs` as the
  library root.
- `cargo package --list` to confirm no UI/dev/fixture files leak; add `exclude` if
  needed. Confirm the crate builds with **default features only** (the
  `postgres-it` / `conformance-it` features are test-only and already off).

### 2. Define the deliberate embedding API (the public contract)
Files: `crates/headwaters/src/lib.rs`; visibility audits across `read/`, `writer/`,
`projection/`, `ingest/`.

- Keep re-exporting `headwaters_proto::{connect_gen, headwaters, lineage}`.
- Promote a small, named embedding surface to the crate root with crate-level
  `//!` rustdoc showing the embed recipe:
  - `config::{Config, PostgresConfig, WriterConfig, UiConfig}`
  - `http::{router, AppState}`
  - `read::{LineageStore, ReadError}`
  - `writer::{BufferedWriter, BufferedWriterHandle, BufferedWriterConfig}` and the
    `writer::sink::{EventSink, SinkError, EventRow}` seam
  - `writer::postgres::PostgresSink` (the good default)
  - `projection::Projector`
  - `ingest::{convert_event, convert_batch}` (pure parsing, reusable)
- **Demote anything currently `pub` we are not committing to** down to
  `pub(crate)` *before* first publish (cheap now, breaking later). `read/mod.rs`
  internals are already `pub(crate)` — good; confirm `read::http::router` exposure
  is intentional (it is — for mounting). Use the **rustdoc** skill for the docs.

### 3. Enable bring-your-own pools (separate read/write)
Files: docs + example only; optionally a thin `headwaters::embed` helper.

- No core API change required — `PostgresSink::new(pool)`, `Projector::spawn(pool,
  ..)`, and `LineageStore::new(pool)` already take pools independently. The example
  simply passes different pools. **Document this** as a first-class capability.
- Expose a small `headwaters::migrate(&pool)` helper that runs the bundled
  `sqlx::migrate!()` against the write pool (the migrations live in this crate, so
  this is friendlier than asking embedders to reach for the migrator themselves).
- Optional thin sugar: a `headwaters::embed` module that assembles `AppState` from
  a caller-provided writer handle + store (mirroring `main.rs` lines 50–66). Keep
  it sugar-only over existing calls.

### 4. Make multi-sink fan-out safe (turn the sink into a real extension point)
File: `crates/headwaters/src/writer/buffered.rs` (+ `writer/sink.rs` docs).

- Rewrite `append_all` → `append_to(pending, rows) -> Vec<failed sinks>` (return
  the sinks that failed this round instead of a `bool`).
- In `flush`, retry only the still-failing sinks: a succeeded sink drops out of
  `pending` and never re-receives the batch → no double-insert *within a flush
  call*. Buffer-clear / retain-for-retry semantics otherwise unchanged.
- Delete the `debug_assert!(sinks.len() <= 1)` guard in `spawn`.
- **Document the contract** on `EventSink`: delivery is **exactly-once on the
  common path** (all sinks succeed in one flush), but **at-least-once across the
  re-buffer boundary** — if sink A succeeds and sink B exhausts retries, the batch
  is retained and re-flushed on the next trigger, so A may see it again. Custom
  sinks MUST be idempotent on retry (dedupe on the event UUID / a natural key). The
  built-in `PostgresSink` is unaffected (single source-of-truth sink, common
  path). Per-sink **persisted** high-water marks to close the cross-trigger window
  are deliberately out of scope at this stage.
- **Tests** (extend the existing `CountingSink` / `FailingSink` harness): two
  healthy sinks each written exactly once; one sink fails-then-recovers beside a
  healthy peer → healthy sink not double-written within the flush, all events
  eventually land in both; existing single-sink retain/drain tests unchanged.

### 5. Refactor `main.rs` to consume the public API (dogfood it)
Files: `crates/headwaters/src/main.rs` (+ possibly `src/run.rs`).

- Build the standalone server **only through the public embedding API** — the same
  calls an external embedder makes — so the published surface is provably
  sufficient and standalone behavior stays byte-for-byte. Keep tracing init,
  signal handling, and process lifecycle in the binary, not the library.
- Coordinate with [cli-server-consolidation-design.md](cli-server-consolidation-design.md)
  § 1, which also wants `main()`'s body extracted into a callable `run`. Do it once
  and share it.

### 6. Add an embedding example + docs
Files: `crates/headwaters/examples/embed.rs`, `crates/headwaters/README.md`,
`lib.rs` crate-level `//!`.

- `examples/embed.rs`: a minimal axum app that `.merge()`s
  `headwaters::http::router(state, "")` into a parent router with one extra route,
  using only `headwaters::` re-exports. Gate the DB connection behind an env var so
  `cargo build --examples` compiles without a live DB (it must *compile* — it
  documents the recipe).
- Document the bring-your-own-pool and bring-your-own-sink (`impl EventSink`) paths
  explicitly, including the at-least-once retry contract.

### 7. Simplify the Docker release trigger
Files: `release-plz.toml`, `.github/workflows/release-plz.yml`,
`crates/headwaters/Cargo.toml`, `CONTRIBUTING.md`.

- `release-plz.toml`: remove the `[[package]] name = "headwaters"` `git_only` /
  `publish = false` override so it publishes like the other libs. (Leave
  `headwaters-cli` as-is — out of scope.)
- `release-plz.yml`: delete the bespoke version-extraction + tag-existence steps
  (~lines 104–146) and instead read the headwaters entry out of release-plz's
  `releases` output to decide whether to invoke `docker-release.yml` and with what
  `version` / `ref`.
- Update the `release-plz.toml` header, the `headwaters` Cargo.toml comment, and
  `CONTRIBUTING.md` to describe the new path: headwaters publishes, its tag
  triggers Docker.
- **Confirm UI/Docker coupling is unaffected:** the Docker build still builds the
  bundled UI and `crates/headwaters/ui.lock` / `just ui-fingerprint` still gates
  it. Publishing changes only what *triggers* the image build, not the build.

## Out of scope (deferred)

- Read-store / projection-source trait abstraction (Delta/DuckDB *read* backends).
  Large refactor; revisit once the read API is proven end-to-end.
- Crate split (separate pipeline/core crate).
- Publishing `headwaters-cli`.
- Per-sink **persisted** high-water marks (the cross-trigger at-least-once window).
  The in-flush fix lands; persisted dedup is deferred — custom sinks are documented
  as needing retry-idempotency instead.

## Verification (for the executing session)

1. **Builds & lints:** `cargo build -p headwaters` (lib + bin),
   `cargo build -p headwaters --examples`, `cargo fmt --all`,
   `cargo clippy --all-targets --all-features -- -D warnings`.
2. **Packaging:** `cargo package -p headwaters --list` (no UI/dev/fixture leak; bin
   + lib present); `cargo publish -p headwaters --dry-run` publishes cleanly.
3. **Embed compiles against the public surface:** `examples/embed.rs` uses only
   `headwaters::` re-exports (no private paths) and compiles.
4. **Multi-sink fan-out:** `cargo test -p headwaters writer::buffered` — new
   two-sink tests pass (both written once; healthy sink not double-written when its
   peer fails-then-recovers); existing single-sink tests unchanged.
5. **Standalone server unchanged:** `cargo run -p headwaters` (with `DATABASE_URL`
   at a `just pg-up` Postgres), POST to `/api/v1/lineage` → `202`, read back via
   `/api/v1/...` and `/version` — identical to pre-change. `postgres-it` tests
   (`cargo test -p headwaters --features postgres-it`, needs Docker) still pass.
6. **docs.rs render:** `cargo doc -p headwaters --no-deps` — eyeball the crate-level
   embed recipe + re-exported items.
7. **Release plumbing (review):** `release-plz.toml` no longer marks headwaters
   `git_only`; the `release-plz.yml` diff drives the Docker job off the `releases`
   output (manifest-version steps gone). `cargo-semver-checks` now applies to
   headwaters — it will gate future breaking changes to the new public API.
