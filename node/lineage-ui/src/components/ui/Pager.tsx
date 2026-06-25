import { ChevronLeft, ChevronRight } from "lucide-react";
import { cn } from "../../lib/cn.js";

export interface PagerProps {
  offset: number;
  limit: number;
  total: number;
  onChange: (offset: number) => void;
}

/** Offset/limit pagination matching the read API's `limit`/`offset` params. */
export function Pager({ offset, limit, total, onChange }: PagerProps) {
  const start = total === 0 ? 0 : offset + 1;
  const end = Math.min(offset + limit, total);
  const canPrev = offset > 0;
  const canNext = end < total;

  const btn =
    "inline-flex h-8 w-8 items-center justify-center rounded-md border border-border text-muted-foreground enabled:hover:bg-muted disabled:opacity-40";

  return (
    <div className="flex items-center justify-end gap-3 px-4 py-2 text-sm text-muted-foreground">
      <span>
        {start}–{end} of {total}
      </span>
      <button
        type="button"
        className={cn(btn)}
        disabled={!canPrev}
        onClick={() => onChange(Math.max(0, offset - limit))}
        aria-label="Previous page"
      >
        <ChevronLeft className="h-4 w-4" />
      </button>
      <button
        type="button"
        className={cn(btn)}
        disabled={!canNext}
        onClick={() => onChange(offset + limit)}
        aria-label="Next page"
      >
        <ChevronRight className="h-4 w-4" />
      </button>
    </div>
  );
}
