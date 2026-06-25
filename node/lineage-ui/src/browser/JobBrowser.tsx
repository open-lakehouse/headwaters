import { Cog } from "lucide-react";
import { useState } from "react";
import { AsyncBoundary } from "../components/ui/AsyncBoundary.js";
import { Pager } from "../components/ui/Pager.js";
import { RunStateBadge } from "../components/ui/RunStateBadge.js";
import { useJobs } from "../hooks/queries.js";
import { cn } from "../lib/cn.js";

export interface JobBrowserProps {
  /** Optional namespace filter. */
  namespace?: string;
  /** Page size (read API `limit`). */
  limit?: number;
  /** Called when a job row is selected, with its `{namespace, name}`. */
  onSelect?: (job: { namespace: string; name: string }) => void;
}

/**
 * A paginated list of jobs (read API `ListJobs`), each showing its latest run
 * state. The scaffold app mounts this at `/jobs`.
 */
export function JobBrowser({
  namespace,
  limit = 50,
  onSelect,
}: JobBrowserProps) {
  const [offset, setOffset] = useState(0);
  const { data, isLoading, error } = useJobs({ namespace, limit, offset });
  const jobs = data?.jobs ?? [];

  return (
    <div className="flex h-full flex-col">
      <AsyncBoundary
        isLoading={isLoading}
        error={error}
        isEmpty={jobs.length === 0}
        emptyMessage="No jobs found. Ingest some OpenLineage run events to populate this view."
      >
        <ul className="flex-1 divide-y divide-border overflow-auto">
          {jobs.map((j) => (
            <li key={`${j.namespace}/${j.name}`}>
              <button
                type="button"
                onClick={() =>
                  onSelect?.({ namespace: j.namespace, name: j.name })
                }
                className={cn(
                  "flex w-full items-center gap-3 px-4 py-3 text-left hover:bg-muted",
                )}
              >
                <Cog className="h-4 w-4 shrink-0 text-violet-500" />
                <span className="min-w-0 flex-1">
                  <span className="block truncate font-medium">
                    {j.simpleName || j.name}
                  </span>
                  <span className="block truncate text-xs text-muted-foreground">
                    {j.namespace}
                  </span>
                </span>
                {j.latestRun && <RunStateBadge state={j.latestRun.state} />}
              </button>
            </li>
          ))}
        </ul>
        <Pager
          offset={offset}
          limit={limit}
          total={data?.totalCount ?? jobs.length}
          onChange={setOffset}
        />
      </AsyncBoundary>
    </div>
  );
}
