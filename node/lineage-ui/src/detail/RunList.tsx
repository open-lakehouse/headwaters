import type { RunDetail } from "@headwaters/lineage-client";
import { RunStateBadge } from "../components/ui/RunStateBadge.js";

function duration(ms: bigint): string {
  const n = Number(ms);
  if (!n) return "—";
  if (n < 1000) return `${n} ms`;
  if (n < 60_000) return `${(n / 1000).toFixed(1)} s`;
  return `${(n / 60_000).toFixed(1)} min`;
}

export interface RunListProps {
  runs: RunDetail[];
}

/** A job's run history: state, timing, and duration per run. */
export function RunList({ runs }: RunListProps) {
  if (runs.length === 0) {
    return <p className="text-sm text-muted-foreground">No runs recorded.</p>;
  }
  return (
    <ul className="flex flex-col gap-2">
      {runs.map((r) => (
        <li
          key={r.id}
          className="flex items-center justify-between gap-3 rounded-md border border-border px-3 py-2 text-sm"
        >
          <RunStateBadge state={r.state} />
          <span
            className="min-w-0 flex-1 truncate text-xs text-muted-foreground"
            title={r.id}
          >
            {r.startedAt || r.createdAt}
          </span>
          <span className="shrink-0 tabular-nums text-xs text-muted-foreground">
            {duration(r.durationMs)}
          </span>
        </li>
      ))}
    </ul>
  );
}
