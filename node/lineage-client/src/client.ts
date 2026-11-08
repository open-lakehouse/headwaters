// The typed ConnectRPC client for headwaters' read API.
//
// `lineage-ui` and the scaffold app obtain a client via `createReadClient()` and
// call methods like `client.listJobs({ limit: 50 })` or
// `client.getLineage({ nodeId, depth: 10 })`. Every call routes through
// `clientTransport` unless given a transport, and that resolves on every call
// (transport.ts): a `registerTransport` override, the `@open-lakehouse/host`
// host's `headwaters` binding, or the page's own origin.

import type { Transport } from "@connectrpc/connect";
import { type Client, createClient } from "@connectrpc/connect";
import { ReadService } from "./gen/headwaters/read/v1/service_pb.js";
import { clientTransport } from "./transport.js";

/** A fully-typed client for `headwaters.read.v1.ReadService`. */
export type ReadClient = Client<typeof ReadService>;

/**
 * Create a read-API client.
 *
 * With no argument it uses `clientTransport`, which resolves the transport on
 * every call (see transport.ts). Pass `transportFor(host)` to bind it to a
 * particular host, or any `transport` to bypass the resolution entirely.
 */
export function createReadClient(
  transport: Transport = clientTransport,
): ReadClient {
  return createClient(ReadService, transport);
}
