import type { StatBucket } from "@headwaters/lineage-client";

export interface StatsViewProps {
  title: string;
  buckets: StatBucket[];
}

/**
 * A compact bar chart of a stats series (`{date, count}` buckets from the read
 * API's stats endpoints). Counts are int64 (bigint over the wire); rendered with
 * plain CSS bars — no chart dependency for the landing page.
 */
export function StatsView({ title, buckets }: StatsViewProps) {
  if (buckets.length === 0) {
    return (
      <div className="rounded-lg border border-border p-4">
        <h3 className="text-sm font-semibold">{title}</h3>
        <p className="mt-2 text-sm text-muted-foreground">
          No activity recorded.
        </p>
      </div>
    );
  }

  const counts = buckets.map((b) => Number(b.count));
  const max = Math.max(1, ...counts);
  const total = counts.reduce((a, b) => a + b, 0);

  return (
    <div className="rounded-lg border border-border p-4">
      <div className="flex items-baseline justify-between">
        <h3 className="text-sm font-semibold">{title}</h3>
        <span className="text-xs text-muted-foreground">{total} total</span>
      </div>
      <div className="mt-4 flex h-40 items-end gap-1">
        {buckets.map((b, i) => (
          <div
            key={b.date}
            className="group flex h-full min-w-0 flex-1 flex-col items-center justify-end"
            title={`${b.date}: ${b.count}`}
          >
            <div
              className="w-full rounded-t bg-primary/70 transition-colors group-hover:bg-primary"
              style={{ height: `${(counts[i] / max) * 100}%` }}
            />
          </div>
        ))}
      </div>
      <div className="mt-1 flex justify-between text-[10px] text-muted-foreground">
        <span>{formatDate(buckets[0]?.date)}</span>
        <span>{formatDate(buckets[buckets.length - 1]?.date)}</span>
      </div>
    </div>
  );
}

function formatDate(iso: string | undefined): string {
  if (!iso) return "";
  return iso.slice(0, 10);
}
