import {
  DatasetDetailPanel,
  JobDetailPanel,
  type LineageNode,
  LineageView,
} from "@headwaters/lineage-ui";
import { getRouteApi, useNavigate } from "@tanstack/react-router";
import { useState } from "react";

const route = getRouteApi("/lineage");

// Parse `dataset:<ns>:<name>` / `job:<ns>:<name>` from a clicked node, preferring
// the node's own `data.id` (which already splits namespace/name correctly).
function parseSelection(
  node: LineageNode,
): { kind: string; namespace: string; name: string } | null {
  const kind = node.id.split(":", 1)[0];
  if (kind !== "dataset" && kind !== "job") return null;
  const data = node.data as
    | { id?: { namespace?: string; name?: string } }
    | undefined;
  if (data?.id?.namespace && data.id.name) {
    return { kind, namespace: data.id.namespace, name: data.id.name };
  }
  return null;
}

export function LineagePage() {
  const { nodeId } = route.useSearch();
  const navigate = useNavigate();
  const [selected, setSelected] =
    useState<ReturnType<typeof parseSelection>>(null);

  if (!nodeId) {
    return (
      <div className="flex h-full flex-col">
        <header className="border-b border-border px-6 py-4">
          <h1 className="text-lg font-semibold tracking-tight">Lineage</h1>
        </header>
        <div className="p-6 text-sm text-muted-foreground">
          Select a dataset or job to view its lineage.
        </div>
      </div>
    );
  }

  const viewLineage = (id: string) =>
    navigate({ to: "/lineage", search: { nodeId: id } });

  return (
    <div className="flex h-full">
      <div className="min-w-0 flex-1">
        <LineageView
          nodeId={nodeId}
          selectedId={
            selected
              ? `${selected.kind}:${selected.namespace}:${selected.name}`
              : undefined
          }
          onSelect={(n) => setSelected(parseSelection(n))}
        />
      </div>
      {selected && (
        <aside className="w-96 shrink-0 overflow-auto border-l border-border">
          {selected.kind === "dataset" ? (
            <DatasetDetailPanel
              namespace={selected.namespace}
              name={selected.name}
              onViewLineage={viewLineage}
              onClose={() => setSelected(null)}
            />
          ) : (
            <JobDetailPanel
              namespace={selected.namespace}
              name={selected.name}
              onViewLineage={viewLineage}
              onClose={() => setSelected(null)}
            />
          )}
        </aside>
      )}
    </div>
  );
}
