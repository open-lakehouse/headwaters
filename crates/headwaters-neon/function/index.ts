// Neon Function entry point: a thin fetch handler over the headwaters
// Node-API addon (`../src/lib.rs`). Every request is forwarded, whole, into the
// in-process axum router; nothing here knows about routes.
//
// Kept to erasable TypeScript (no enums/namespaces/parameter properties) so the
// build can emit `index.mjs` with Node's built-in type stripping — no bundler,
// no npm dependencies. See `just neon-build`.

import { createRequire } from "node:module";

interface Header {
  name: string;
  value: string;
}

interface NativeRequest {
  method: string;
  url: string;
  headers: Header[];
  body: Buffer;
}

interface NativeResponse {
  status: number;
  headers: Header[];
  body: Buffer;
}

interface Addon {
  start(): Promise<void>;
  handle(request: NativeRequest): Promise<NativeResponse>;
  shutdown(): Promise<void>;
}

const addon = createRequire(import.meta.url)("./native/headwaters.node") as Addon;

// Statuses the fetch `Response` constructor rejects a body for.
const NULL_BODY_STATUSES = new Set([101, 204, 205, 304]);

// Start lazily on the first request and share the promise across concurrent
// ones. A failed start (e.g. pending migrations) is retried on the next request.
let started: Promise<void> | undefined;
function ready(): Promise<void> {
  started ??= addon.start().catch((err: unknown) => {
    started = undefined;
    throw err;
  });
  return started;
}

// Neon sends SIGINT before evicting an isolate and kills it 5 s later: flush
// the buffered writer (ingest answers 202 before events hit Postgres) first.
process.once("SIGINT", () => {
  addon.shutdown().finally(() => process.exit(0));
});

export default {
  async fetch(request: Request): Promise<Response> {
    try {
      await ready();
    } catch (err) {
      console.error("headwaters failed to start:", err);
      return new Response("headwaters failed to start; see function logs\n", { status: 503 });
    }

    const url = new URL(request.url);
    const response = await addon.handle({
      method: request.method,
      url: url.pathname + url.search,
      headers: [...request.headers].map(([name, value]) => ({ name, value })),
      body: Buffer.from(await request.arrayBuffer()),
    });

    const body =
      NULL_BODY_STATUSES.has(response.status) || request.method === "HEAD" ? null : response.body;
    return new Response(body, {
      status: response.status,
      headers: response.headers.map(({ name, value }): [string, string] => [name, value]),
    });
  },
};
