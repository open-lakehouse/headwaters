import { HomeView } from "@headwaters/lineage-ui";

export function HomePage() {
  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-border px-6 py-4">
        <h1 className="text-lg font-semibold tracking-tight">Home</h1>
      </header>
      <div className="min-h-0 flex-1 overflow-auto">
        <HomeView />
      </div>
    </div>
  );
}
