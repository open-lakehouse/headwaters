import { Database } from "lucide-react";
import { useState } from "react";
import { AsyncBoundary } from "../components/ui/AsyncBoundary.js";
import { Pager } from "../components/ui/Pager.js";
import { useDatasets } from "../hooks/queries.js";
import { cn } from "../lib/cn.js";

export interface DatasetBrowserProps {
  /** Optional namespace filter. */
  namespace?: string;
  /** Page size (read API `limit`). */
  limit?: number;
  /** Called when a dataset row is selected, with its `{namespace, name}`. */
  onSelect?: (dataset: { namespace: string; name: string }) => void;
}

/**
 * A paginated list of datasets (read API `ListDatasets`). The reusable browser
 * the scaffold app mounts at `/datasets` and that a host app can embed.
 */
export function DatasetBrowser({
  namespace,
  limit = 50,
  onSelect,
}: DatasetBrowserProps) {
  const [offset, setOffset] = useState(0);
  const { data, isLoading, error } = useDatasets({ namespace, limit, offset });
  const datasets = data?.datasets ?? [];

  return (
    <div className="flex h-full flex-col">
      <AsyncBoundary
        isLoading={isLoading}
        error={error}
        isEmpty={datasets.length === 0}
        emptyMessage="No datasets found. Ingest some OpenLineage events to populate this view."
      >
        <ul className="flex-1 divide-y divide-border overflow-auto">
          {datasets.map((d) => (
            <li key={`${d.namespace}/${d.name}`}>
              <button
                type="button"
                onClick={() =>
                  onSelect?.({ namespace: d.namespace, name: d.name })
                }
                className={cn(
                  "flex w-full items-center gap-3 px-4 py-3 text-left hover:bg-muted",
                )}
              >
                <Database className="h-4 w-4 shrink-0 text-sky-500" />
                <span className="min-w-0 flex-1">
                  <span className="block truncate font-medium">{d.name}</span>
                  <span className="block truncate text-xs text-muted-foreground">
                    {d.namespace}
                    {d.description ? ` · ${d.description}` : ""}
                  </span>
                </span>
                {d.tags.length > 0 && (
                  <span className="shrink-0 text-xs text-muted-foreground">
                    {d.tags.join(", ")}
                  </span>
                )}
              </button>
            </li>
          ))}
        </ul>
        <Pager
          offset={offset}
          limit={limit}
          total={data?.totalCount ?? datasets.length}
          onChange={setOffset}
        />
      </AsyncBoundary>
    </div>
  );
}
