import { LineageView } from "@headwaters/lineage-ui";
import { getRouteApi } from "@tanstack/react-router";

const route = getRouteApi("/lineage");

export function LineagePage() {
  const { nodeId } = route.useSearch();

  if (!nodeId) {
    return (
      <div className="flex h-full flex-col">
        <header className="border-b border-border px-6 py-4">
          <h1 className="text-lg font-semibold">Lineage</h1>
        </header>
        <div className="p-6 text-sm text-muted-foreground">
          Select a dataset or job to view its lineage.
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-border px-6 py-3">
        <h1 className="text-lg font-semibold">Lineage</h1>
      </header>
      <div className="min-h-0 flex-1">
        <LineageView nodeId={nodeId} />
      </div>
    </div>
  );
}
