// Placeholder pages for surfaces delivered in later stages (lineage graph,
// search, home/stats). Each is replaced in Stage 2 / Stage 3.

function Placeholder({ title, note }: { title: string; note: string }) {
  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-border px-6 py-4">
        <h1 className="text-lg font-semibold">{title}</h1>
      </header>
      <div className="p-6 text-sm text-muted-foreground">{note}</div>
    </div>
  );
}

export function HomePage() {
  return <Placeholder title="Home" note="Activity stats land in Stage 3." />;
}

export function SearchPage() {
  return <Placeholder title="Search" note="Search lands in Stage 3." />;
}
