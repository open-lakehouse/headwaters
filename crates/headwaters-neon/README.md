# headwaters-neon

Packages headwaters as a [Neon Function](https://neon.com/docs/compute/functions/overview).
It has two parts:

- a Node-API addon, `src/lib.rs`, that wraps the in-process axum router
- a short fetch handler, `function/index.ts`

The design rationale and the measured sizes are in
[ADR 0015](../../docs/adr/0015-neon-functions-deployment-via-napi.md).

## Prerequisites

- `zig`
- `cargo-zigbuild` 0.23 or newer
- Node 22.13 or newer, for type stripping
- Docker, for the arm64 test
- the `neon` CLI, for deploying

## Build and test

```bash
just neon-build        # linux-arm64 bundle → crates/headwaters-neon/headwaters-neon.zip (size-gated)
just neon-test         # host-native addon, end-to-end against the local dev Postgres
just neon-test-arm64   # the deployable bundle, in node:24-bookworm-slim
```

`neon-test` and `neon-test-arm64` start the dev Postgres on `PG_PORT` (default
5432). If that port is already taken, override it, for example
`just PG_PORT=55440 neon-test`.

## Deploy

```bash
DATABASE_URL_UNPOOLED='postgres://…' just neon-migrate   # once per schema change; direct URL
just neon-deploy                                          # or: just neon-deploy my-slug
just neon-trigger                                         # once per branch; same slug
```

## Projection

Neon only keeps an isolate's work alive while it handles requests or has
`waitUntil` work pending, so the function folds the event log on demand
instead of in a background loop
([ADR 0016](../../docs/adr/0016-projection-liveness-and-topology-on-neon.md)):

- After each accepted ingest, the handler calls the addon's `settle`, which
  waits for the writer to flush and then folds. It registers that work with
  `waitUntil`, so the isolate stays alive until the fold finishes. Concurrent
  ingests share one settle.
- The `neon-trigger` cron (every minute) POSTs to `/internal/projection/run`.
  The handler accepts it only when Neon's `X-Neon-Trigger-Invocation-Id`
  header is present (Neon strips that header from client requests) and folds
  for up to 50 s. This catches events left behind when an isolate is evicted.

## Runtime configuration

At runtime the function reads the same `HEADWATERS__*` env vars and
`DATABASE_URL` as the standalone binary. Neon injects `DATABASE_URL` (pooled)
and `DATABASE_URL_UNPOOLED` (direct). The direct URL is used only for the
startup schema check, unless `HEADWATERS__POSTGRES__SCHEMA_CHECK_URL` is set.
Projection defaults to `on_demand` here; set
`HEADWATERS__POSTGRES__PROJECTION_MODE=background` to bring back the polling
loop. The bundled UI is never served. Logs follow `RUST_LOG` (default
`headwaters=info`).
