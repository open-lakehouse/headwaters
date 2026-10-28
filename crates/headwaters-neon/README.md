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
```

## Runtime configuration

At runtime the function reads the same `HEADWATERS__*` env vars and
`DATABASE_URL` as the standalone binary. Neon injects `DATABASE_URL` (pooled)
and `DATABASE_URL_UNPOOLED` (direct). The direct URL is used only for the
startup schema check, unless `HEADWATERS__POSTGRES__SCHEMA_CHECK_URL` is set.
The bundled UI is never served. Logs follow `RUST_LOG` (default
`headwaters=info`).
