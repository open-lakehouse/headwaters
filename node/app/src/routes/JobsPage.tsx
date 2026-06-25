import { JobBrowser } from "@headwaters/lineage-ui";
import { useNavigate } from "@tanstack/react-router";

export function JobsPage() {
  const navigate = useNavigate();
  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-border px-6 py-4">
        <h1 className="text-lg font-semibold tracking-tight">Jobs</h1>
      </header>
      <div className="min-h-0 flex-1">
        <JobBrowser
          onSelect={({ namespace, name }) =>
            navigate({
              to: "/lineage",
              search: { nodeId: `job:${namespace}:${name}` },
            })
          }
        />
      </div>
    </div>
  );
}
