import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The local lineage-service (`just lineage`, default :8091) serves both the REST
// read API and the ConnectRPC read API on one port. In dev we proxy the
// ConnectRPC path prefix to it so the browser talks to a single origin (the Vite
// dev server) — the registered transport (see main.tsx) uses a relative baseUrl.
//
// Connect RPC paths are rooted at the fully-qualified service name.
const LINEAGE_URL = process.env.LINEAGE_URL ?? "http://localhost:8091";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 3010,
    proxy: {
      "/headwaters.read.v1.ReadService": {
        target: LINEAGE_URL,
        changeOrigin: true,
      },
    },
  },
});
