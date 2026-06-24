# Publishing `datafusion-open-lineage`

This crate is being prepared for a standalone crates.io release (and an eventual move
to its own repository). It is **not published yet**. This file is the checklist to get
there.

## Pre-publish checklist

- [ ] **Metadata** — `Cargo.toml` carries `version`, `description`, `license`,
      `repository`, `homepage`, `documentation`, `readme`, `keywords`, `categories`.
- [ ] **Only published dependencies.** The package's non-dev dependencies must all be
      on crates.io — no `git`/`path` deps. Currently true: `datafusion` (53.1),
      `datafusion-common`, `datafusion-expr`, `olai-http` (0.0.1), `reqwest`, `serde`,
      `serde_json`, `chrono`, `uuid`, `futures`, `url`, `tokio`, `tracing`,
      `thiserror`, `async-trait`, `serde_yaml`. (The wider workspace pins delta-rs /
      unitycatalog git forks, but this crate does not depend on them.)
- [ ] **Dry run** — packages cleanly:
      ```sh
      cargo publish --dry-run -p datafusion-open-lineage
      ```
      Note: the vendored JSON Schemas under `tests/schemas/` are test fixtures. They
      are included in the package (under the default include rules) but harmless; add
      an `exclude` to `Cargo.toml` if package size matters.
- [ ] **Docs build** — `cargo doc -p datafusion-open-lineage --no-deps` is clean.
- [ ] **Both test layers pass** (see below).
- [ ] **CHANGELOG** seeded for the first release.
- [ ] License headers / `LICENSE` file present at the published root once extracted.

## Tests

Always-on (no Docker, run in CI):

```sh
cargo test -p datafusion-open-lineage
cargo test --doc -p datafusion-open-lineage
```

Reference-backend acceptance (needs Docker; run locally / in a dedicated CI job):

```sh
cargo test -p datafusion-open-lineage --features marquez-it -- --ignored
```

## Dependency audit notes

- **`olai-http`** — published on crates.io at `0.0.1` (the only version); the workspace
  already pins exactly that, so nothing to bump. It is what makes the default `http`
  feature publishable.
- **DataFusion `53.1`** — newer DataFusion releases exist. Bumping is a **workspace-wide**
  change (every crate pins 53.1) and is intentionally out of scope for the release-prep
  pass; track it separately.

## Spec version maintenance

The crate stamps specific OpenLineage facet versions into each `_schemaURL`
(`src/builder.rs`, `src/exec.rs`, `src/extract.rs`, `src/context.rs`). They currently
match the latest published versions. When a new OpenLineage spec release advances a
facet, bump the emitted constant **and** re-vendor the schema under
`tests/schemas/openlineage/` in the same change, then re-run `tests/conformance.rs`. The
two must never drift — the conformance test only proves we conform if it validates
against the versions we actually advertise.

## Known behavior to document for users

In-memory `CREATE TABLE AS SELECT` is intercepted by DataFusion before physical
planning, so its output dataset does not flow through the instrumented planner (the
`START`/`COMPLETE` pair carries inputs only). DML writes (`INSERT` / `DELETE` /
`UPDATE`) and file/table-provider-backed writes flow output datasets and column
lineage end-to-end. This is a property of where DataFusion routes DDL, not a bug in
the adapter.
