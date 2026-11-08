# @open-lakehouse/lineage-ui

Composable, host-agnostic React components for the Headwaters lineage read API.

This is the reusable feature package. The scaffold app (`node/app`) is one
host. The desktop shell, or any app that implements `@open-lakehouse/host`,
mounts the same components with no changes here.

## The integration contract

Import **only** from the package barrel (`@open-lakehouse/lineage-ui`), never from
internal paths. The Biome `noRestrictedImports` rule in `node/biome.json`
enforces this for in-repo consumers.

### Public surface (the barrel)

**Client injection**
- `ReadClientProvider`, `useReadClient`, `useReadScope`: mount the provider
  once. Every hook and component reads the injected `ReadClient` and the cache
  scope from context.

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
- `src/testing/*` (Storybook fixtures + the fake host; not published)

## How a host mounts it

```bash
npm install @open-lakehouse/lineage-ui @open-lakehouse/lineage-client @open-lakehouse/host
```

Both lineage packages are on GitHub Packages, so map the scope in `.npmrc`:
`@open-lakehouse:registry=https://npm.pkg.github.com`, with a token that has
`read:packages`. `@open-lakehouse/host`, `react` and `react-dom` are peers.

Components never construct a transport or a client. They read the `ReadClient`
that `ReadClientProvider` injects. Without a `client` prop, the provider builds
one over the [`@open-lakehouse/host`][host] host's `headwaters` binding. A host
that implements the contract therefore only mounts the providers:

```tsx
import { registerHost } from "@open-lakehouse/host";
import { LineageView, ReadClientProvider } from "@open-lakehouse/lineage-ui";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

registerHost(myHost); // in main.ts, before the UI is imported

const queryClient = new QueryClient();

function LineagePanel({ nodeId }: { nodeId: string }) {
  return (
    <QueryClientProvider client={queryClient}>
      <ReadClientProvider>
        <LineageView nodeId={nodeId} />
      </ReadClientProvider>
    </QueryClientProvider>
  );
}
```

The provider picks the client in this order:

1. its `client` prop;
2. a transport installed with `registerTransport` from
   `@open-lakehouse/lineage-client`, the explicit override (the scaffold app
   uses it to root RPCs at its base path);
3. the `headwaters` binding of a `<HostProvider host>` above it, or else of the
   registered host;
4. with no host at all, the page's own origin.

If the environment doesn't bind `headwaters`, queries fail with
`ServiceUnavailableError`, so gate the panel on
`useService("headwaters")` from `@open-lakehouse/host/react`.

Query keys include the provider's `scope`, which defaults to the host's
environment key. After an environment switch the hooks refetch instead of
showing the previous environment's cached data, and the host doesn't have to
clear the query cache. Pass `scope` to partition the cache yourself.

The same components run unchanged in three hosts:
- **the scaffold app** (`node/app`): no host, and `registerTransport` with the
  server's base path;
- **Storybook**: a `createFakeHost` from `@open-lakehouse/host/testing` whose
  `headwaters` binding is an in-memory router over the fixtures (see
  `src/testing/fake-client.tsx`);
- **the desktop shell** (luminescent-lakehouse): `registerHost`, and nothing
  lineage-specific.

[host]: https://github.com/open-lakehouse/trestle/tree/main/node/host

## Styling

Components use Tailwind utility classes against a small set of semantic tokens
(`background`, `foreground`, `muted`, `muted-foreground`, `border`) plus a few
accent colors. A host's Tailwind build **must scan this package's source** so
those utilities are generated — with Tailwind 4, add to the host stylesheet:

```css
@source "../node_modules/@open-lakehouse/lineage-ui/src/**/*.{ts,tsx}";
```

(In this repo the scaffold uses a relative `@source "../../lineage-ui/src/..."`.)
The host should also import `@xyflow/react/dist/style.css` for the graph canvas.
