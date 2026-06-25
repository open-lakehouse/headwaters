// Pluggable ConnectRPC transport registry — the single seam that lets a host
// environment route the read API's RPC calls somewhere other than the network,
// WITHOUT the UI taking on any dependency on that host.
//
// By default this speaks Connect over the platform fetch against the current
// origin, so a normal web build behaves exactly as a direct client. A host can
// call `registerTransport` before the UI bootstraps to swap in its own
// implementation:
//   - the scaffold app registers a `createConnectTransport({ baseUrl })`;
//   - Storybook registers an in-memory fixture transport;
//   - hydrofoil (later) registers its own transport pointed at its gateway.
//
// `lineage-ui` components NEVER construct a transport — they read `clientTransport`
// (via `createReadClient`). This file is the only place that knows how RPCs reach
// the wire, so the path is fully replaceable.
//
// Deliberately framework-agnostic: no `import.meta.env`, no globals beyond the
// default transport's fetch.

import type { Transport } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

// Default: Connect-over-fetch against the current origin. An empty base resolves
// against the dev origin; a Vite proxy forwards the RPC path prefix
// (/headwaters.read.v1.ReadService/*) to the lineage-service. See the app's
// vite.config.ts.
const defaultTransport: Transport = createConnectTransport({
  baseUrl: "/",
});

let currentTransport: Transport = defaultTransport;

/** Install a custom transport. Hosts call this once, before the UI bootstraps. */
export function registerTransport(t: Transport): void {
  currentTransport = t;
}

/** The transport currently in effect (the registered one, or the default). */
export function getTransport(): Transport {
  return currentTransport;
}

// Stable, late-binding transport handed to ConnectRPC clients. Each method
// dereferences `currentTransport` on every call, so registration order relative
// to client construction never matters — a host can register before OR after the
// client is created and still take effect.
export const clientTransport: Transport = {
  unary(...args) {
    return currentTransport.unary(...args);
  },
  stream(...args) {
    return currentTransport.stream(...args);
  },
};
