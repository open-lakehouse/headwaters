import { AsyncBoundary } from "../components/ui/AsyncBoundary.js";
import { useDataset } from "../hooks/queries.js";
import { DetailSection, MetaRow } from "./DetailSection.js";
import { SchemaTable } from "./SchemaTable.js";

export interface DatasetDetailPanelProps {
  namespace: string;
  name: string;
  /** Optional: render a "View lineage" affordance. */
  onViewLineage?: (nodeId: string) => void;
}

/** A dataset's metadata, schema, and tags (read API `GetDataset`). */
export function DatasetDetailPanel({
  namespace,
  name,
  onViewLineage,
}: DatasetDetailPanelProps) {
  const { data: dataset, isLoading, error } = useDataset(namespace, name);

  return (
    <AsyncBoundary isLoading={isLoading} error={error}>
      {dataset && (
        <div className="overflow-auto">
          <header className="flex items-start justify-between gap-3 px-5 py-4">
            <div className="min-w-0">
              <h2
                className="truncate text-base font-semibold"
                title={dataset.name}
              >
                {dataset.name}
              </h2>
              <p
                className="truncate text-sm text-muted-foreground"
                title={dataset.namespace}
              >
                {dataset.namespace}
              </p>
            </div>
            {onViewLineage && (
              <button
                type="button"
                className="shrink-0 rounded-md border border-border px-2.5 py-1 text-sm hover:bg-muted"
                onClick={() =>
                  onViewLineage(`dataset:${dataset.namespace}:${dataset.name}`)
                }
              >
                View lineage
              </button>
            )}
          </header>

          <DetailSection title="Details">
            <MetaRow label="Type" value={dataset.type} />
            <MetaRow label="Physical name" value={dataset.physicalName} />
            <MetaRow label="Source" value={dataset.sourceName} />
            <MetaRow label="Updated" value={dataset.updatedAt} />
            {dataset.description && (
              <MetaRow label="Description" value={dataset.description} />
            )}
          </DetailSection>

          {dataset.tags.length > 0 && (
            <DetailSection title="Tags">
              <div className="flex flex-wrap gap-1.5">
                {dataset.tags.map((t) => (
                  <span
                    key={t}
                    className="rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground"
                  >
                    {t}
                  </span>
                ))}
              </div>
            </DetailSection>
          )}

          <DetailSection title="Schema">
            <SchemaTable fields={dataset.fields} />
          </DetailSection>
        </div>
      )}
    </AsyncBoundary>
  );
}
