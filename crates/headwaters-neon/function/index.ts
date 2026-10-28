// Neon Function entry point: a thin fetch handler over the headwaters
// Node-API addon (`../src/lib.rs`). Every request is forwarded, whole, into the
// in-process axum router. The only routes known here drive projection, which
// runs on demand (ADR 0016): ingest schedules a settle via `waitUntil`, and a
// cron trigger settles on PROJECTION_PATH.
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

interface ProjectionRun {
  applied: number;
  caughtUp: boolean;
}

interface Addon {
  start(): Promise<void>;
  handle(request: NativeRequest): Promise<NativeResponse>;
  settle(budgetMs: number): Promise<ProjectionRun>;
  shutdown(): Promise<void>;
}

// What `@neon/functions`' `waitUntil` wraps; reading it directly keeps the
// function free of npm dependencies. Undefined off-platform, where the promise
// still runs, just untracked.
interface NeonRequestContext {
  waitUntil?: (promise: Promise<unknown>) => void;
}
const neon = globalThis as { NEON_REQUEST_CONTEXT?: NeonRequestContext };

const addon = createRequire(import.meta.url)("./native/headwaters.node") as Addon;

// Statuses the fetch `Response` constructor rejects a body for.
const NULL_BODY_STATUSES = new Set([101, 204, 205, 304]);

// Same path as the server's Bearer-guarded trigger route. Requests carrying
// Neon's trigger header are settled here; anything else falls through to the
// router.
const PROJECTION_PATH = "/internal/projection/run";
// Suffixes, so ingest under a `ui.base_path` matches too.
const INGEST_PATHS = ["/api/v1/lineage", "/api/v1/lineage/batch"];
// Neon strips client-supplied `X-Neon-*` headers at the edge, so its presence
// proves the request is a trigger delivery.
const TRIGGER_HEADER = "x-neon-trigger-invocation-id";
// The cron fires every minute; stop well before the next one.
const TRIGGER_BUDGET_MS = 50_000;
const INGEST_BUDGET_MS = 10_000;

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

// Settle after ingest, coalesced: while one settle runs, further ingests ask
// for one more pass instead of stacking their own.
let settling: Promise<void> | undefined;
let rerun = false;
function settleSoon(): Promise<void> {
  if (settling) {
    rerun = true;
    return settling;
  }
  settling = (async () => {
    do {
      rerun = false;
      try {
        await addon.settle(INGEST_BUDGET_MS);
      } catch (err) {
        console.error("headwaters projection failed:", err);
      }
    } while (rerun);
  })().finally(() => {
    settling = undefined;
  });
  return settling;
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
    if (
      url.pathname === PROJECTION_PATH &&
      request.method === "POST" &&
      request.headers.has(TRIGGER_HEADER)
    ) {
      try {
        return Response.json(await addon.settle(TRIGGER_BUDGET_MS));
      } catch (err) {
        console.error("headwaters projection failed:", err);
        return new Response("projection failed; see function logs\n", { status: 500 });
      }
    }

    const response = await addon.handle({
      method: request.method,
      url: url.pathname + url.search,
      headers: [...request.headers].map(([name, value]) => ({ name, value })),
      body: Buffer.from(await request.arrayBuffer()),
    });

    if (INGEST_PATHS.some((p) => url.pathname.endsWith(p)) && response.status < 300) {
      // Started unconditionally; `waitUntil` keeps the isolate alive until the
      // accepted events are folded.
      const settled = settleSoon();
      neon.NEON_REQUEST_CONTEXT?.waitUntil?.(settled);
    }

    const body =
      NULL_BODY_STATUSES.has(response.status) || request.method === "HEAD" ? null : response.body;
    return new Response(body, {
      status: response.status,
      headers: response.headers.map(({ name, value }): [string, string] => [name, value]),
    });
  },
};
