import { defaultClientConditions } from "vite";
import { defineConfig } from "vitest/config";

// Unit tests for the workspace packages. Workspace imports resolve through the
// `headwaters-source` export condition to `src/`, as in the app (see
// app/vite.config.ts), so nothing is built first.
export default defineConfig({
  resolve: { conditions: ["headwaters-source", ...defaultClientConditions] },
  test: {
    include: ["*/src/**/*.test.{ts,tsx}"],
  },
});
