import type { ReactNode } from "react";

/** A titled section in a detail panel. */
export function DetailSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="border-b border-border px-5 py-4">
      <h3 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
        {title}
      </h3>
      {children}
    </section>
  );
}

/** A label/value row for entity metadata. */
export function MetaRow({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="flex gap-3 py-0.5 text-sm">
      <span className="w-28 shrink-0 text-muted-foreground">{label}</span>
      <span className="min-w-0 break-words">{value}</span>
    </div>
  );
}
