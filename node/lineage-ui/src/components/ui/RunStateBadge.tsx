import { cn } from "../../lib/cn.js";

// Marquez run states: NEW | RUNNING | COMPLETED | FAILED | ABORTED.
const STATE_STYLES: Record<string, string> = {
  COMPLETED:
    "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-300",
  RUNNING: "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300",
  NEW: "bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
  FAILED: "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300",
  ABORTED:
    "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300",
};

export interface RunStateBadgeProps {
  state: string;
  className?: string;
}

/** A small pill rendering an OpenLineage run state with state-specific color. */
export function RunStateBadge({ state, className }: RunStateBadgeProps) {
  const style = STATE_STYLES[state] ?? STATE_STYLES.NEW;
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium",
        style,
        className,
      )}
    >
      {state || "UNKNOWN"}
    </span>
  );
}
