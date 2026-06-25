// The typed ConnectRPC client for headwaters' read API.
//
// `lineage-ui` and the scaffold app obtain a client via `createReadClient()` and
// call methods like `client.listJobs({ limit: 50 })` or
// `client.getLineage({ nodeId, depth: 10 })`. Every call routes through
// `clientTransport`, the late-binding seam in transport.ts, so the same client
// works against the network, Storybook fixtures, or a host gateway depending on
// what (if anything) was registered via `registerTransport`.

import type { Transport } from "@connectrpc/connect";
import { type Client, createClient } from "@connectrpc/connect";
import { ReadService } from "./gen/headwaters/read/v1/service_pb.js";
import { clientTransport } from "./transport.js";

/** A fully-typed client for `headwaters.read.v1.ReadService`. */
export type ReadClient = Client<typeof ReadService>;

/**
 * Create a read-API client.
 *
 * With no argument it uses `clientTransport` (the registered transport, or the
 * default Connect-over-fetch). Pass an explicit `transport` to bind a client to
 * a specific transport — used by tests and by hosts that manage their own.
 */
export function createReadClient(
  transport: Transport = clientTransport,
): ReadClient {
  return createClient(ReadService, transport);
}
