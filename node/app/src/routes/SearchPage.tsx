import { SearchView } from "@headwaters/lineage-ui";
import { getRouteApi, useNavigate } from "@tanstack/react-router";

const route = getRouteApi("/search");

export function SearchPage() {
  const { q } = route.useSearch();
  const navigate = useNavigate();

  return (
    <div className="flex h-full flex-col">
      <header className="border-b border-border px-6 py-4">
        <h1 className="text-lg font-semibold">Search</h1>
      </header>
      <div className="min-h-0 flex-1">
        <SearchView
          query={q ?? ""}
          onQueryChange={(next) =>
            navigate({
              to: "/search",
              search: { q: next || undefined },
              replace: true,
            })
          }
          onSelect={({ nodeId }) =>
            navigate({ to: "/lineage", search: { nodeId } })
          }
        />
      </div>
    </div>
  );
}
