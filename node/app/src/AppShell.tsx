import { Link, Outlet } from "@tanstack/react-router";
import { Cog, Database, GitBranch, Home, Search } from "lucide-react";
import type { ReactNode } from "react";
import { ThemeToggle } from "./components/ThemeToggle.js";

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
      className="flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground [&.active]:bg-accent [&.active]:text-accent-foreground"
      activeOptions={{ exact: to === "/" }}
    >
      <Icon className="h-4 w-4" />
      {label}
    </Link>
  );
}

export function AppShell({ children }: { children?: ReactNode }) {
  return (
    <div className="flex h-full flex-col">
      <header className="sticky top-0 z-50 flex h-12 shrink-0 items-center justify-between border-b border-border bg-background/80 px-4 backdrop-blur-sm">
        <span className="text-sm font-semibold tracking-tight">Headwaters</span>
        <ThemeToggle />
      </header>
      <div className="flex min-h-0 flex-1">
        <aside className="flex w-56 shrink-0 flex-col gap-1 border-r border-border bg-sidebar p-2 text-sidebar-foreground">
          <div className="px-3 py-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            Lineage
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
    </div>
  );
}
