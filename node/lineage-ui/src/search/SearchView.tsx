import { Cog, Database, Search } from "lucide-react";
import { AsyncBoundary } from "../components/ui/AsyncBoundary.js";
import { useSearch } from "../hooks/queries.js";

export interface SearchViewProps {
  /** The query string (controlled by the host, e.g. from a URL param). */
  query: string;
  /** Called when the query input changes. */
  onQueryChange: (q: string) => void;
  /** Called when a result is selected, with its nodeId + type. */
  onSelect?: (result: { nodeId: string; type: string }) => void;
}

/**
 * Free-text search over jobs and datasets (read API `Search`), with a results
 * list that deep-links by nodeId.
 */
export function SearchView({
  query,
  onQueryChange,
  onSelect,
}: SearchViewProps) {
  const { data, isLoading, error } = useSearch(query);
  const results = data?.results ?? [];

  return (
    <div className="flex h-full flex-col">
      <div className="border-b border-border p-4">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <input
            type="search"
            value={query}
            onChange={(e) => onQueryChange(e.target.value)}
            placeholder="Search jobs and datasets…"
            className="w-full rounded-md border border-border bg-background py-2 pl-9 pr-3 text-sm outline-none focus:border-ring"
          />
        </div>
      </div>
      <div className="min-h-0 flex-1">
        {query.trim().length === 0 ? (
          <div className="p-6 text-sm text-muted-foreground">
            Type to search across jobs and datasets.
          </div>
        ) : (
          <AsyncBoundary
            isLoading={isLoading}
            error={error}
            isEmpty={results.length === 0}
            emptyMessage={`No results for “${query}”.`}
          >
            <ul className="divide-y divide-border">
              {results.map((r) => {
                const Icon = r.type === "JOB" ? Cog : Database;
                return (
                  <li key={r.nodeId}>
                    <button
                      type="button"
                      onClick={() =>
                        onSelect?.({ nodeId: r.nodeId, type: r.type })
                      }
                      className="flex w-full items-center gap-3 px-4 py-3 text-left hover:bg-muted"
                    >
                      <Icon className="h-4 w-4 shrink-0 text-muted-foreground" />
                      <span className="min-w-0 flex-1">
                        <span className="block truncate font-medium">
                          {r.name}
                        </span>
                        <span className="block truncate text-xs text-muted-foreground">
                          {r.namespace}
                        </span>
                      </span>
                      <span className="shrink-0 text-xs text-muted-foreground">
                        {r.type}
                      </span>
                    </button>
                  </li>
                );
              })}
            </ul>
          </AsyncBoundary>
        )}
      </div>
    </div>
  );
}
