import { ReadClientProvider } from "@headwaters/lineage-ui";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { createRouter, RouterProvider } from "@tanstack/react-router";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { ThemeProvider } from "./components/ThemeProvider.js";
import "./globals.css";
import { routeTree } from "./routeTree.js";

// The default lineage-client transport already speaks Connect over fetch against
// the current origin, and the Vite proxy forwards the ReadService path prefix to
// headwaters (see vite.config.ts). So the scaffold needs no explicit
// registerTransport — the default IS the network path. Storybook overrides it
// with a fixture transport; a host app would register its own here.

const queryClient = new QueryClient({
  defaultOptions: { queries: { staleTime: 10_000, retry: 1 } },
});

const router = createRouter({ routeTree });

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
