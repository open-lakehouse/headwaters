import {
  createDefaultTransport,
  registerTransport,
} from "@headwaters/lineage-client";
import { ReadClientProvider } from "@headwaters/lineage-ui";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createRouter, RouterProvider } from "@tanstack/react-router";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ThemeProvider } from "./components/ThemeProvider.js";
import "./globals.css";
import { routeTree } from "./routeTree.js";

// The URL prefix the service is served under, injected into index.html by the
// server (see crates/headwaters/src/http.rs). Empty string = served at root,
// which is the default and the dev-server case (the Vite proxy forwards the
// ReadService path prefix to headwaters; see vite.config.ts). When non-empty
// (e.g. "/lineage") both the client-side router and the RPC transport are rooted
// there so deep links and RPCs resolve under the prefix.
const basePath = window.__HEADWATERS_BASE_PATH__ ?? "";

// Point the RPC transport at the prefix. `createConnectTransport`'s baseUrl is
// resolved against the origin (it does NOT honor <base href>), so the prefix
// must be threaded in explicitly; empty falls back to "/" — identical to the
// previous default. Storybook overrides this with a fixture transport.
registerTransport(createDefaultTransport(basePath || "/"));

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 10_000, retry: 1 } },
});

// Root the client-side router at the prefix so <Link>s and deep-link reloads
// stay under it. Empty basepath is the root deployment (unchanged behavior).
const router = createRouter({ routeTree, basepath: basePath || undefined });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

const root = document.getElementById("root");
if (!root) throw new Error("missing #root");

createRoot(root).render(
  <StrictMode>
    <ThemeProvider>
      <QueryClientProvider client={queryClient}>
        <ReadClientProvider>
          <RouterProvider router={router} />
        </ReadClientProvider>
      </QueryClientProvider>
    </ThemeProvider>
  </StrictMode>,
);
