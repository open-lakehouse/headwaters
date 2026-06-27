# Contributing to Headwaters

Thanks for your interest in contributing! This guide covers the local setup and
the conventions CI enforces.

## Prerequisites

- A Rust toolchain matching the workspace MSRV (`rust-version` in the root
  `Cargo.toml`; currently 1.91). Newer stable is fine for day-to-day work — the
  `msrv` CI job verifies the floor still builds.
- [`just`](https://just.systems) for the common recipes (`just --list`).
- Docker, for the integration tests and the local dev environment.
- [`buf`](https://buf.build), only if you regenerate protobuf code.

## Build & test

```sh
just build          # cargo build --workspace --all-features
just test           # cargo nextest run --workspace --all-features
```

The default test run needs no services: live-integration tests are `#[ignore]`d
and the offline OpenLineage spec-conformance suite validates against vendored
JSON Schemas. The Docker-gated suites:

```sh
just postgres-it    # headwaters read/projection tests (Postgres via testcontainers)
just conformance-it # differential conformance against the real Marquez
just marquez-it      # open-lineage acceptance test against the Marquez reference backend
```

## Local dev environment

```sh
just dev            # Postgres + headwaters, clean start
just seed           # post the demo lineage graph (see examples/seed/README.md)
just dev-down       # tear it down
```

For the web UI (`node/`), see [`node/README.md`](node/README.md) and the
`just ui-*` recipes.

## Before you push

CI gates on formatting, clippy (all warnings denied), tests, doc warnings, and
the MSRV build. Run the same checks locally:

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo doc --no-deps                  # must be warning-free (datafusion-open-lineage ships to docs.rs)
```

### Protobuf changes

The generated Rust (`crates/headwaters/src/{proto,connect_gen}/`) and the
TypeScript client (`node/lineage-client/src/gen/`) are committed so the
workspace builds without a codegen step. If you edit anything under `proto/`,
regenerate and commit the output in the same change:

```sh
just proto-gen      # Rust types
just ui-gen         # TypeScript client
```

CI fails if the committed output drifts from the `.proto` sources.

## Commit & PR conventions

- **Conventional commits.** PR titles must follow
  [Conventional Commits](https://www.conventionalcommits.org)
  (`feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, …, with an optional
  `(scope)`); a CI check enforces this, and releases are derived from the
  history by [release-plz](https://release-plz.dev). Prefer several small,
  well-scoped commits over one large mixed one.
- **Branch from `main`** and open a pull request; do not push to `main`.
- **Releases are automated.** Don't bump crate versions or edit `CHANGELOG.md`
  by hand — release-plz maintains both from the merged commit history.

## License

By contributing, you agree that your contributions are licensed under the
[Apache-2.0](LICENSE) license.
