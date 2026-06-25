import { Link, Outlet } from "@tanstack/react-router";
import { Cog, Database, GitBranch, Home, Search } from "lucide-react";
import type { ReactNode } from "react";

const NAV = [
  { to: "/", label: "Home", icon: Home },
  { to: "/datasets", label: "Datasets", icon: Database },
  { to: "/jobs", label: "Jobs", icon: Cog },
  { to: "/lineage", label: "Lineage", icon: GitBranch },
  { to: "/search", label: "Search", icon: Search },
] as const;

function NavItem({
  to,
  label,
  icon: Icon,
}: {
  to: string;
  label: string;
  icon: typeof Home;
}) {
  return (
    <Link
      to={to}
      className="flex items-center gap-3 rounded-md px-3 py-2 text-sm text-muted-foreground hover:bg-muted hover:text-foreground [&.active]:bg-muted [&.active]:text-foreground [&.active]:font-medium"
      activeOptions={{ exact: to === "/" }}
    >
      <Icon className="h-4 w-4" />
      {label}
    </Link>
  );
}

export function AppShell({ children }: { children?: ReactNode }) {
  return (
    <div className="flex h-full">
      <aside className="flex w-56 shrink-0 flex-col gap-1 border-r border-border p-3">
        <div className="px-3 py-2 text-sm font-semibold tracking-tight">
          Headwaters
        </div>
        <nav className="flex flex-col gap-0.5">
          {NAV.map((item) => (
            <NavItem key={item.to} {...item} />
          ))}
        </nav>
      </aside>
      <main className="min-w-0 flex-1 overflow-hidden">
        {children ?? <Outlet />}
      </main>
    </div>
  );
}
