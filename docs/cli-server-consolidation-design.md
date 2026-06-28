# CLI / server consolidation — technical design & handover

> How the `headwaters-cli` (`hw`) binary should relate to the `headwaters`
> server binary: whether `hw` stays a lean client or also gains the ability to
> *start* the server, how that maps onto Docker images and published crates, and
> what to build first. This is a **design + handover document** — no code ships
> with it. It captures a decided architecture so a later session can execute
> against it; work is currently deferred in favor of other priorities in this
> repo. No ADR is filed yet — promote the core decision to an ADR when
> implementation begins.

## Why

We recently introduced `headwaters-cli` (the `hw` binary; `crates/headwaters-cli`)
— a thin client over `headwaters-client` for inspecting a lineage estate, for
humans and agents. That raised an architectural question: the CLI could
eventually also be the entry point for *starting the server*, and we may
distribute a Docker container that works either as a CLI or to start the server.

The tension is dependency weight:

- The **server** (`headwaters`, `crates/headwaters`) pulls a heavy stack —
  `sqlx` + Postgres + migrations, `axum`, `connectrpc/server`, the projection
  worker, and a bundled single-page UI.
- The **CLI** (`headwaters-cli`) today pulls only `headwaters-client` + `clap` +
  `comfy-table` — small, fast-building, and `cargo install`-friendly.

Naively merging them would force the entire server stack onto every CLI user.
The question this document answers: keep them strictly separate, or produce a
consolidated full-featured `hw` that can also start the server?

## Decision

**Keep the crates strictly separate, and give the CLI an opt-in, off-by-default
`server` feature that adds a `hw serve` subcommand.** Do not fold server logic
into the client library, and do not make the lean CLI depend on the server by
default.

This yields three things at once:

1. Default `hw` stays lean (client-only) — fast builds, small, eventually
   `cargo install`-able; crates.io users never pull `sqlx`/`axum` unless they
   opt in.
2. A single full-featured binary exists for the experimenter path
   (`hw --features server` → `hw serve`).
3. The operator-grade distroless **server-only** image and its release path stay
   unchanged.

Rejected alternatives:

- **Strictly separate, two unrelated binaries** — loses the single-artifact
  convenience the experimenter / Docker story wants. The feature gate buys that
  convenience nearly for free (see enabling facts below).
- **Always-consolidated single binary** — bloats every `hw` install and a future
  `cargo install headwaters-cli` with the server stack, slows CLI builds, and
  couples the lean publishable CLI to the heavy unpublished server.
- **Server gains a *client* feature** — wrong direction. `headwaters` is the
  heavy, currently-unpublished crate; the *lean* crate (CLI) is the right home
  for an optional capability that opts *up* into weight, never one that forces
  weight *down*.

### Enabling facts (verified 2026-06-28)

These make the chosen design cheap and shape its constraints:

- **The server is already a library + thin binary.**
  `crates/headwaters/src/lib.rs` exposes `config/http/ingest/projection/read/writer`;
  `src/main.rs` is just a `#[tokio::main]` wrapper. Embedding "start the server"
  into `hw` requires no restructuring of server internals — only extracting
  `main()`'s body into a callable `run` function.
- **The UI is served from `./web` on disk**, via `ServeDir`/`ServeFile`
  (`crates/headwaters/src/http.rs`, `const UI_DIR = "web"`), and 404s gracefully
  when the bundle is absent. The Dockerfile copies `node/app/dist` → `./web`.
  There is **no** `serve-ui` Cargo feature today (a stale Dockerfile comment
  references one). Consequence: **the UI travels with the Docker image, not with
  the binary.**
- **The `Dockerfile` is already multi-stage** (`chef` → `planner` → `ui` →
  `builder` → `runtime`) with a single final target. A second consolidated
  target slots in and *reuses* the shared `chef`/`planner`/`ui` stages — the lean
  CLI build is a subset of the consolidated build, so the dependency graph (via
  cargo-chef) is shared and only the final `cargo build` line differs.

### Distribution decisions

- **One Dockerfile, multiple targets** (not a second Dockerfile). The existing
  distroless server target stays the default and byte-for-byte unchanged; the
  consolidated image is an explicit `--target` build that reuses shared stages.
- **Both `headwaters` (server) and `headwaters-cli` will eventually be
  published**, but publishing is **deferred** until the architecture settles.
  Both remain `git_only = true` for now (see `release-plz.toml`). The plan must
  not bake in "git_only forever".
- **The combined `hw serve` *UI* experience is Docker-only for now.** A
  standalone `cargo install`ed binary has no `./web` next to it. Embedding the UI
  in the binary (see "Optional later refinement") would lift that restriction;
  it is out of scope for the first cut.

## Implementation plan

### 1. Extract a `run` entry point in the `headwaters` library
Files: `crates/headwaters/src/lib.rs` (+ a new `src/run.rs`), `src/main.rs`.

- Move the body of `main()` — config load → Postgres pool → migrations → spawn
  `BufferedWriter` + `Projector` → `axum::serve` with graceful shutdown → drain
  — into `pub async fn run(config_path: Option<String>) -> anyhow::Result<()>`.
  Keep the existing `writer_config` and `shutdown_signal` helpers beside it.
- Shrink `src/main.rs` to: init tracing, read the first positional arg, call
  `headwaters::run(arg).await`. Existing server binary behavior is unchanged.
- Keep tracing initialization in the **binaries** (`headwaters` and `hw`), not in
  `run`, so the library never fights a host that already installed a subscriber.

### 2. Add the `server` feature + `serve` subcommand to the CLI
Files: `crates/headwaters-cli/Cargo.toml`, `src/cli.rs`, `src/commands/`,
root `Cargo.toml`.

```toml
# crates/headwaters-cli/Cargo.toml
[features]
default = []
server = ["dep:headwaters", "dep:anyhow"]

[dependencies]
headwaters = { workspace = true, optional = true }   # add to [workspace.dependencies]
anyhow = { workspace = true, optional = true }
```

- `src/cli.rs`: add `Serve(ServeArgs)` to the subcommand enum, gated
  `#[cfg(feature = "server")]`. `ServeArgs` carries an optional config path that
  mirrors the server's positional arg; reuse the server's existing env precedence
  (`HEADWATERS_CONFIG`, `HEADWATERS__*`, `DATABASE_URL`).
- `src/commands/serve.rs` (new, `#[cfg(feature = "server")]`): a thin wrapper
  that calls `headwaters::run(args.config)`. Wire it into the `commands::run`
  dispatch behind the same `cfg`. Map the server's `anyhow::Result` into the
  CLI's existing `error.rs` exit-code scheme so `hw serve` failures report
  consistently with the other subcommands.

### 3. Dockerfile: add a consolidated target reusing shared stages
File: `Dockerfile` (single file, new targets).

- Reuse `chef` / `planner` / `ui` unchanged. Add a builder target that runs
  `cargo build --release -p headwaters-cli --features server`, and a second
  `runtime` target that lays down the `hw` binary plus the `./web` bundle from
  the `ui` stage, with `ENTRYPOINT ["hw"]` — so `docker run … serve` starts the
  server and `docker run … lineage …` runs client subcommands.
- Keep the existing server `runtime` as the **default** target so today's
  `docker build` invocations and the operator image are unchanged; the
  consolidated image is built with an explicit `--target`.

### 4. Release wiring (deferred, but publish-ready)
Files: `release-plz.toml`, `.github/workflows/`.

- Leave `headwaters` and `headwaters-cli` as `git_only = true` for now (matches
  the "defer publishing" decision). When ready: flip the CLI to crates.io — the
  default-off `server` feature keeps the published artifact lean — and publish
  the server crate alongside it. Until the server crate is on crates.io, document
  that `hw serve` is a from-source/Docker capability (and the UI is Docker-only
  until embedding lands).
- If the consolidated image should publish on CLI version bumps, reuse the
  existing manifest-version → Docker-tag trigger pattern (release-plz omits
  `git_only` crates from its `releases` output, so the tag is derived in a
  dedicated workflow step — same as the `headwaters-v<version>` → Docker flow in
  `release-plz.yml` → `docker-release.yml`), keyed on `headwaters-cli-v<version>`.

### CI note
Add `cargo build -p headwaters-cli --features server` to the build matrix so the
gated `serve` path can't bit-rot. `cargo clippy --all-targets --all-features`
already covers it locally per the repo hygiene rules in `CONTRIBUTING.md`.

## Optional later refinement — UI-in-binary

Add a `serve-ui` feature to `headwaters` that embeds `node/app/dist` (via
`rust-embed` or `include_dir`) and serves it from memory when `./web` is absent.
That would make the consolidated **and UI** experience available from a
standalone `cargo install`ed `hw`, not just Docker. The branch point is the
`serve_ui` construction in `crates/headwaters/src/http.rs` (`router_in`), where
the on-disk `ServeDir` vs. an embedded service would be selected. Out of scope
for the first cut.

## Verification (for the executing session)

1. **Default CLI unchanged & lean:** `cargo tree -p headwaters-cli` (no feature)
   shows no `sqlx`/`axum`; `cargo build -p headwaters-cli` succeeds; `hw
   namespaces` still works against a running server.
2. **Consolidated build works:** `cargo build -p headwaters-cli --features
   server` succeeds; `cargo run -p headwaters-cli --features server -- serve`
   (with `DATABASE_URL` pointed at a `just pg-up` Postgres) serves
   `/api/v1/lineage` + the ConnectRPC read surface identically to
   `cargo run -p headwaters`.
3. **Server binary unchanged:** `just dev` / `cargo run -p headwaters` behaves
   exactly as before — the extracted `run` is the only code path.
4. **Docker:** the default (server) target builds the same distroless image as
   today; the consolidated `--target` image can both `serve` (UI from `./web`)
   and run client subcommands (`docker run … hw lineage …`).
5. **Round-trip:** with the consolidated image `serve`-ing, run `hw lineage
   <target>` / `hw namespaces` against it and confirm output matches the
   standalone-server case.
