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
just neon-trigger                                         # once per branch; same slug, every 10 min
just neon-trigger headwaters '*/30 * * * *'               # or pick the cadence (UTC cron, ≥ 1 min)
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
- The `neon-trigger` cron POSTs to `/internal/projection/run`. The handler
  accepts it only when Neon's `X-Neon-Trigger-Invocation-Id` header is present
  (Neon strips that header from client requests) and folds for up to the
  trigger budget. This catches events left behind when an isolate is evicted.

### Choosing the cadence

Three settings trade read freshness against compute:

| Setting | Default | Effect |
|---|---|---|
| `neon-trigger` cron | `*/10 * * * *` | How often the trigger folds. Change it later with `neon triggers update <trigger-id> --cron '…'`. |
| `HEADWATERS_NEON_SETTLE_ON_INGEST` | `true` | `false`: ingest only waits for the writer flush. Folding is left to the cron, so reads lag by up to one interval, and each fold is one large batch rather than many small ones. |
| `HEADWATERS_NEON_TRIGGER_BUDGET_MS` | `50000` | How long one trigger may fold (at most 840000, under Neon's 15 min response limit). Raise it with the interval when ingest doesn't settle, since the backlog grows with the interval. |

- **Settle on ingest (default):** reads are fresh within about a second, and
  the cron only covers isolate evictions. A slow cadence costs little.
- **Cron only:** reads lag by up to one cron interval. Use it when freshness
  doesn't matter and ingest is bursty.
- **Postgres compute:** every trigger run queries the branch. A cadence under
  Neon's 5 minute scale-to-zero window keeps the compute from ever
  suspending, even with no traffic. Ingest still wakes it as usual.

A malformed `HEADWATERS_NEON_*` value fails the start (503, logged).

## Runtime configuration

At runtime the function reads the same `HEADWATERS__*` env vars and
`DATABASE_URL` as the standalone binary. Neon injects `DATABASE_URL` (pooled)
and `DATABASE_URL_UNPOOLED` (direct). The direct URL is used only for the
startup schema check, unless `HEADWATERS__POSTGRES__SCHEMA_CHECK_URL` is set.
Projection defaults to `on_demand` here; set
`HEADWATERS__POSTGRES__PROJECTION_MODE=background` to bring back the polling
loop. The bundled UI is never served. Logs follow `RUST_LOG` (default
`headwaters=info`).
