# Headwaters lineage UI (`node/`)

An npm-workspaces monorepo for the Headwaters lineage UI, co-located with the
Rust service and proto (mirrors the layout in the sibling `hydrofoil` and
`workflows` repos). Three packages:

| Package | What it is |
| --- | --- |
| [`@headwaters/lineage-client`](./lineage-client) | Generated ConnectRPC TypeScript client + types for the read API, plus the late-binding transport seam. The analog of hydrofoil's `uc-client`. Generated code is committed under `src/gen/`. |
| [`@headwaters/lineage-ui`](./lineage-ui) | The reusable React feature: graph canvas, browsers, detail panels, search, stats. The integration surface a host (e.g. hydrofoil) consumes. See its [README](./lineage-ui/README.md) for the public/internal contract. |
| `@headwaters/lineage-app` (`app/`) | A thin scaffold app + Storybook. Runs against a local `lineage-service`. |

## Quick start

From the repo root (recipes in the `justfile`):

```bash
just ui-install              # npm install across the workspace
just ui-gen                  # regenerate the read-API TS client from proto/

# run against a live service:
DATABASE_URL=postgres://… just lineage   # start lineage-service on :8091 (separate terminal)
just ui-dev                  # Vite dev server on :3010, proxying ConnectRPC to :8091

just ui-sb                   # Storybook (mocked, no backend)
just ui-check                # tsc -b + biome (what CI gates on)
```

## How data flows

The read API is served as **ConnectRPC** by `lineage-service` (alongside REST,
on one port). `lineage-client` generates a typed client from the same `proto/`
module the Rust crate uses — one proto, two language clients. Components obtain
that client through `ReadClientProvider`, which rides a late-binding transport
seam, so the **same components run against the network, Storybook fixtures, or a
host gateway** depending only on what the host registers. That seam is what makes
`lineage-ui` reusable without modification.
