// How the read API's RPCs reach headwaters. The lineage UI never constructs a
// transport; it calls a client over `clientTransport` (or over
// `transportFor(host)`), which picks the transport again on every call:
//
//   1. a transport passed explicitly to `createReadClient(transport)`
//      (that one never comes through here);
//   2. the one installed with `registerTransport`, the explicit override a host
//      can still use (the standalone app does, to root RPCs at its base path);
//   3. the `headwaters` binding of the `@open-lakehouse/host` host: its
//      ConnectRPC `transport`, or else Connect over its `fetch`;
//   4. with no host at all, Connect over the platform fetch against the page's
//      own origin.
//
// A host that is there but doesn't bind headwaters fails the call with the
// host's `ServiceUnavailableError` rather than guessing an origin. Since every
// call resolves again, registering a transport or a host before or after a
// client is built, or switching environments, needs no new client.

import type { Transport } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import {
  getHost,
  type LakehouseHost,
  type ServiceBinding,
  ServiceUnavailableError,
} from "@open-lakehouse/host";

/** The trestle catalog module id headwaters is bound under. */
export const HEADWATERS_MODULE_ID = "headwaters";

/**
 * Build a Connect-over-fetch transport rooted at `baseUrl` (default `"/"`, the
 * current origin). A host serving the UI under a URL prefix passes that prefix
 * here so RPCs POST to `{prefix}/headwaters.read.v1.ReadService/*` — see the
 * scaffold app's `main.tsx`, which reads the server-injected base path. The
 * client package owns the `@connectrpc/connect-web` dependency, so hosts get a
 * correctly-configured transport without taking it on themselves.
 */
export function createDefaultTransport(baseUrl = "/"): Transport {
  return createConnectTransport({ baseUrl });
}

// Without a host, the page's own origin; a Vite proxy forwards the RPC path
// prefix (/headwaters.read.v1.ReadService/*) to headwaters in dev (see the
// app's vite.config.ts).
const defaultTransport: Transport = createDefaultTransport();

let registered: Transport | null = null;

/**
 * Install a transport that wins over the host's binding, or clear it with
 * `null`. Hosts that implement `@open-lakehouse/host` don't need this.
 */
export function registerTransport(transport: Transport | null): void {
  registered = transport;
}

// Connect over a binding's `fetch`, built once per binding. The read API's
// paths sit at the binding's root: `/headwaters.read.v1.ReadService/<method>`.
const overFetch = new WeakMap<ServiceBinding, Transport>();

function bindingTransport(binding: ServiceBinding): Transport {
  // The host types `transport` structurally; any connect v2 Transport fits.
  if (binding.transport) return binding.transport as Transport;
  let transport = overFetch.get(binding);
  if (!transport) {
    transport = createConnectTransport({ baseUrl: "", fetch: binding.fetch });
    overFetch.set(binding, transport);
  }
  return transport;
}

/** The transport for the next call, given the host in effect (if any). */
function resolve(host: LakehouseHost | null): Transport {
  if (registered) return registered;
  if (!host) return defaultTransport;
  const binding = host.services.get(HEADWATERS_MODULE_ID);
  if (!binding) {
    throw new ServiceUnavailableError(
      HEADWATERS_MODULE_ID,
      "the current environment doesn't bind it",
    );
  }
  return bindingTransport(binding);
}

/**
 * A transport that resolves on every call, against `host` when one is given
 * (a `<HostProvider>`'s, say) and against the registered host otherwise.
 */
export function transportFor(host?: LakehouseHost): Transport {
  const current = () => resolve(host ?? getHost());
  return {
    async unary(...args) {
      return current().unary(...args);
    },
    async stream(...args) {
      return current().stream(...args);
    },
  };
}

/**
 * The transport a call through `clientTransport` would use now. Throws
 * `ServiceUnavailableError` when the registered host doesn't bind headwaters.
 */
export function getTransport(): Transport {
  return resolve(getHost());
}

/** The late-binding transport `createReadClient()` uses by default. */
export const clientTransport: Transport = transportFor();
