// Smoke test for HEADWATERS_NEON_SETTLE_ON_INGEST=false: ingest only flushes
// the writer, and events are folded when the cron trigger fires. The env is
// read when the handler starts, so this file runs in its own process; see
// `just neon-test`.

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import fn from "./dist/index.mjs";

const BASE = "https://headwaters.example";
const namespace = `neon-lazy-${randomUUID()}`;

const call = (path, init) => fn.fetch(new Request(BASE + path, init));

async function namespaces() {
  const res = await call("/api/v1/namespaces");
  assert.equal(res.status, 200);
  return ((await res.json()).namespaces ?? []).map((n) => n.name);
}

test("ingest is folded only by the trigger", async () => {
  assert.equal(process.env.HEADWATERS_NEON_SETTLE_ON_INGEST, "false");
  const ingest = await call("/api/v1/lineage", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      eventType: "COMPLETE",
      eventTime: new Date().toISOString(),
      producer: "https://github.com/open-lakehouse/headwaters/neon-smoke",
      run: { runId: randomUUID() },
      job: { namespace, name: "lazy_job" },
    }),
  });
  assert.equal(ingest.status, 202, await ingest.text());

  // Well past a writer flush (500 ms): the event is written but not folded.
  await new Promise((r) => setTimeout(r, 1500));
  assert.ok(!(await namespaces()).includes(namespace), "folded without a trigger");

  const invocationId = randomUUID();
  const run = await call("/internal/projection/run", {
    method: "POST",
    headers: { "x-neon-trigger-invocation-id": invocationId },
    body: JSON.stringify({ version: 1, invocation_id: invocationId }),
  });
  assert.equal(run.status, 200, await run.clone().text());
  assert.ok((await run.json()).applied >= 1);
  assert.ok((await namespaces()).includes(namespace));
});
