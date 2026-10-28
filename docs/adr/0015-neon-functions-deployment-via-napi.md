# 0015 — Deploy to Neon Functions as a Node-API addon

> Status: **Accepted** (2026-10). Adds `crates/headwaters-neon` and the
> `headwaters::embed` module. Sizes below were measured on 2026-10-27 with
> rustc 1.99.1 and cargo-zigbuild 0.23.4.

## Context

[Neon Functions](https://neon.com/docs/compute/functions/overview) run
serverless request handlers in the same region as a Neon branch, against that
branch's Postgres. That is a natural home for headwaters, whose only stateful
dependency is Postgres. The platform has these constraints:

- **JS/TS only, on Node.js 24.** A function is a module whose default export
  has `fetch(request: Request): Promise<Response>`. No other runtimes or
  container images are supported.
- **linux / arm64 / glibc.** Neon does not document the glibc version.
- **Deploy archive limits: 10 MiB compressed, 64 MiB uncompressed, 4,096
  files.** An archive over any limit fails to deploy.
- **Shipping native code.** A prebuilt directory deploys unbundled
  (`bundler: "none"` / `neon functions deploy --no-bundle`) with `index.mjs`
  at the root. Neon does no architecture check on this path. The code is
  extracted to a read-only directory, and `/tmp` is mounted `noexec`. Neither
  affects `dlopen` of a `.node`.
- **Lifecycle.** Idle isolates are evicted with SIGINT and killed 5 s later.
- **Postgres.** `DATABASE_URL` is pooled (PgBouncer in transaction mode).
  `DATABASE_URL_UNPOOLED` is a direct connection.

Sources: neon.com/docs/compute/functions/{overview, deploy,
environment-variables, reference/runtime-limits}, and Neon's
[native-binaries reference](https://github.com/neondatabase/agent-skills/blob/main/skills/neon-functions/references/native-binaries.md).
Neon labels the filesystem layout as observed behaviour, not documented
behaviour.

## Decision

Compile the existing axum router into a **Node-API addon (napi-rs)** and wrap it
in a fetch handler of about 60 lines that knows nothing about routes. Each
request goes into the router through `tower::ServiceExt::oneshot`, so there is
no socket, no port, and no second process.

- `headwaters::embed::Service` is the socket-less half of `run()`. It connects
  the pool, checks the schema, spawns the buffered writer and the projector, and
  builds the router. `run()` now uses it too, so the binary and the addon share
  one code path.
- `crates/headwaters-neon` (cdylib) exports `start`, `handle` and `shutdown`.
  `handle` buffers the whole request and response. Every endpoint is unary
  today, so nothing needs streaming.
- `crates/headwaters-neon/function/index.ts` starts the service lazily on the
  first request. A failed start returns 503 and is retried on the next request.
  On SIGINT it calls `shutdown`, which drains with a 3 s budget inside Neon's
  5 s window, because ingest answers 202 before events reach Postgres.
  `index.mjs` is produced by Node's built-in type stripping, so the function
  has no npm dependencies and no bundler.
- The startup schema check takes a **session** advisory lock. In transaction
  mode a pooler can't keep that lock on one backend. The new
  `postgres.schema_check_url` option points the check at a direct connection,
  and the addon fills it from `DATABASE_URL_UNPOOLED`. Request traffic stays on
  the pooled URL. Migrations are never applied by the function. Run
  `just neon-migrate` against the direct URL.
- The addon is built with `cargo zigbuild --target
  aarch64-unknown-linux-gnu.2.28`. 2.28 is the glibc floor of the official
  Node 24 linux binaries, so it loads on any host that can run Node 24.
  cargo-zigbuild must be ≥ 0.23, because older versions pass rustc's
  `--fix-cortex-a53-843419` link argument through to zig, which rejects it.

### Size

The zip limit was the main risk. It turned out not to bind. The heavy
dependencies (DataFusion, arrow) lived only in `datafusion-openlineage` (since
moved out of this repo), which the service never linked.

| `[profile.release-neon]` (fat LTO, 1 CGU, strip) | `.node` | zip -9 |
|---|---:|---:|
| `opt-level = "z"` | 5.3 MiB | 2.7 MiB |
| `opt-level = "s"` | 5.8 MiB | 2.7 MiB |
| `opt-level = 3` (**chosen**) | 7.3 MiB | 3.4 MiB |

We keep `opt-level = 3` because it uses about a third of the limit.
`just neon-build` fails if the zip reaches 10 MiB or the uncompressed size
reaches 64 MiB, so any regression surfaces at build time. If space ever gets
tight, the next steps in order are: `opt-level = "z"`, then `panic = "abort"`,
then gating the binary-only dependencies of `headwaters` (clap, blocking
reqwest, tower-http `fs`) behind a default feature. UPX is not an option
because compressed shared objects don't `dlopen` reliably.

The artifact links only libc, libm, libpthread and libdl, and needs at most
`GLIBC_2.28`.

## Alternatives considered

- **Ship the `headwaters` binary and spawn it from Node.** Executables must be
  copied to `/run` (a tmpfs) and given exec permission at startup. This adds a
  second process and a localhost hop. It also means managing the child's
  lifecycle across isolate eviction. The addon avoids all of that, and its
  size is about the same.
- **Compile to WASM.** sqlx, tokio's multi-threaded runtime and rustls-over-TCP
  don't target `wasm32` in a way Node could host without a rewrite of the
  storage layer.
- **Rewrite the service in TypeScript.** This would fork the read and write
  model away from the Rust service.

## Consequences

- Each isolate runs its own projector. The projectors coordinate through a
  row lock on the `projection_state` cursor, so each batch is folded once. The
  background loop uses `SKIP LOCKED`: while another isolate is folding, the
  rest go back to sleep instead of each holding a pooled connection.
- Buffered events are lost if an isolate is killed without SIGINT. This
  matches the existing 202 semantics.
- The bundled UI is not served (`ui.serve = false`). Neon Functions don't host
  websites.
- Verification: `just neon-test` runs the handler on the host, and
  `just neon-test-arm64` runs the shipped linux-arm64 bundle in
  `node:24-bookworm-slim`. Both drive the handler with fetch `Request`s:
  ingest, wait for projection, then read over REST and ConnectRPC. A real
  deploy to a Neon branch has not been done yet.
