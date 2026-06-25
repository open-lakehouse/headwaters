import { createRootRoute, createRoute } from "@tanstack/react-router";
import { AppShell } from "./AppShell.js";
import { DatasetsPage } from "./routes/DatasetsPage.js";
import { JobsPage } from "./routes/JobsPage.js";
import { HomePage, LineagePage, SearchPage } from "./routes/placeholders.js";

const rootRoute = createRootRoute({ component: AppShell });

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: HomePage,
});

const datasetsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/datasets",
  component: DatasetsPage,
});

const jobsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs",
  component: JobsPage,
});

const lineageRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/lineage",
  validateSearch: (search: Record<string, unknown>): { nodeId?: string } => ({
    nodeId: typeof search.nodeId === "string" ? search.nodeId : undefined,
  }),
  component: LineagePage,
});

const searchRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/search",
  validateSearch: (search: Record<string, unknown>): { q?: string } => ({
    q: typeof search.q === "string" ? search.q : undefined,
  }),
  component: SearchPage,
});

export const routeTree = rootRoute.addChildren([
  indexRoute,
  datasetsRoute,
  jobsRoute,
  lineageRoute,
  searchRoute,
]);
