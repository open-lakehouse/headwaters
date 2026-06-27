# @headwaters/lineage-ui

Composable, host-agnostic React components for the Headwaters lineage read API.

This is the reusable feature package — the analog of hydrofoil's `unity-catalog`
feature. The scaffold app (`node/app`) is one host; **hydrofoil (or any app) can
mount these same components** by depending on this package and
`@headwaters/lineage-client`, with zero changes here.

## The integration contract

Import **only** from the package barrel (`@headwaters/lineage-ui`), never from
internal paths. The Biome `noRestrictedImports` rule in `node/biome.json`
enforces this for in-repo consumers.

### Public surface (the barrel)

**Client injection**
- `ReadClientProvider`, `useReadClient` — mount the provider once; every hook
  and component reads the injected `ReadClient` from context.

**Data hooks** (TanStack Query over the read client)
- `useJobs`, `useDatasets`, `useJob`, `useDataset`, `useJobRuns`, `useLineage`,
  `useColumnLineage`, `useSearch`, `useLineageEventStats`, `useAssetStats`,
  `lineageQueryKey`.

**Components**
- Browsers: `DatasetBrowser`, `JobBrowser`
- Graph: `LineageCanvas` (presentational), `LineageView` (data-connected,
  table/column toggle + depth)
- Detail: `DatasetDetailPanel`, `JobDetailPanel`, `RunList`, `SchemaTable`
- Search: `SearchView`
- Home: `HomeView`, `StatsView`
- Primitives: `AsyncBoundary`, `Pager`, `RunStateBadge`

**Helpers / types**
- `datasetNodeId`, `jobNodeId`, `nodeIdKind`
- `datasetNodeData`, `jobNodeData`, `fieldNodeData`, `LineageNodeKind`
- Re-exported read-model types (`Dataset`, `JobDetail`, `LineageGraph`, …) so a
  host needs only this one dependency for the common types.

### Internal (not exported — do not import directly)
- `src/hooks/queries.ts`, `src/hooks/client-context.tsx` internals
- `src/graph/{useLineageLayout,model}.ts`, `src/graph/nodes/*`
- `src/detail/DetailSection.tsx`
- `src/testing/*` (Storybook fixtures + fake client)

## How a host mounts it

Components never construct a transport or a client — they read the injected
`ReadClient`, which itself rides `@headwaters/lineage-client`'s late-binding
transport seam. A host wires it up once:

```tsx
import { QueryClientProvider, QueryClient } from "@tanstack/react-query";
import { ReadClientProvider, LineageView } from "@headwaters/lineage-ui";
// optional: point the client somewhere other than the current origin
import { registerTransport } from "@headwaters/lineage-client";
import { createConnectTransport } from "@connectrpc/connect-web";

// 1. (optional) register a transport before mount — e.g. a host gateway.
registerTransport(createConnectTransport({ baseUrl: "https://lineage.internal" }));

// 2. mount the providers + any lineage-ui component.
function LineagePanel({ nodeId }: { nodeId: string }) {
  return (
    <QueryClientProvider client={new QueryClient()}>
      <ReadClientProvider>
        <LineageView nodeId={nodeId} />
      </ReadClientProvider>
    </QueryClientProvider>
  );
}
```

Three hosts, one component, no changes to `lineage-ui`:
- **scaffold app** — default transport over the current origin (Vite proxy → the
  local `headwaters`);
- **Storybook** — a fake `ReadClient` passed to `ReadClientProvider` (see
  `src/testing/fake-client.tsx`);
- **hydrofoil** — `registerTransport(...)` pointed at its own gateway, or its own
  `ReadClient` passed to the provider.

## Styling

Components use Tailwind utility classes against a small set of semantic tokens
(`background`, `foreground`, `muted`, `muted-foreground`, `border`) plus a few
accent colors. A host's Tailwind build **must scan this package's source** so
those utilities are generated — with Tailwind 4, add to the host stylesheet:

```css
@source "../node_modules/@headwaters/lineage-ui/src/**/*.{ts,tsx}";
```

(In this repo the scaffold uses a relative `@source "../../lineage-ui/src/..."`.)
The host should also import `@xyflow/react/dist/style.css` for the graph canvas.
