import { DatasetBrowser } from "@headwaters/lineage-ui";
import { useNavigate } from "@tanstack/react-router";

export function DatasetsPage() {
  const navigate = useNavigate();
  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-border px-6 py-4">
        <h1 className="text-lg font-semibold">Datasets</h1>
      </header>
      <div className="min-h-0 flex-1">
        <DatasetBrowser
          onSelect={({ namespace, name }) =>
            navigate({
              to: "/lineage",
              search: { nodeId: `dataset:${namespace}:${name}` },
            })
          }
        />
      </div>
    </div>
  );
}
