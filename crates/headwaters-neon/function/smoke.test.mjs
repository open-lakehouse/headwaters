// End-to-end smoke test for the built function bundle (`dist/`), driven the
// way Neon drives it: fetch `Request` in, `Response` out. Needs a migrated
// Postgres in DATABASE_URL; run via `just neon-test` (host) or
// `just neon-test-arm64` (linux-arm64 in Docker).

import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";

import fn from "./dist/index.mjs";

const BASE = "https://headwaters.example";
const namespace = `neon-smoke-${randomUUID()}`;

const call = (path, init) => fn.fetch(new Request(BASE + path, init));

test("health", async () => {
  const res = await call("/health");
  assert.equal(res.status, 200);
});

test("ingest, project, and read back over REST and ConnectRPC", async () => {
  const event = {
    eventType: "COMPLETE",
    eventTime: new Date().toISOString(),
    producer: "https://github.com/open-lakehouse/headwaters/neon-smoke",
    schemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent",
    run: { runId: randomUUID() },
    job: { namespace, name: "smoke_job" },
    inputs: [{ namespace, name: "in_table" }],
    outputs: [{ namespace, name: "out_table" }],
  };
  const ingest = await call("/api/v1/lineage", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(event),
  });
  assert.equal(ingest.status, 202, await ingest.text());

  // The handler settles after ingest (writer flush, default 500 ms, then a
  // fold). Read responses are proto3 JSON, which omits empty repeated fields.
  let names = [];
  for (let i = 0; i < 40 && !names.includes(namespace); i++) {
    await new Promise((r) => setTimeout(r, 250));
    const res = await call("/api/v1/namespaces");
    assert.equal(res.status, 200);
    names = ((await res.json()).namespaces ?? []).map((n) => n.name);
  }
  assert.ok(names.includes(namespace), `namespace ${namespace} never projected`);

  const rpc = await call("/headwaters.read.v1.ReadService/ListJobs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ namespace }),
  });
  assert.equal(rpc.status, 200, await rpc.clone().text());
  const jobs = ((await rpc.json()).jobs ?? []).map((j) => j.name);
  assert.deepEqual(jobs, ["smoke_job"]);
});

test("a trigger delivery settles the projection", async () => {
  const invocationId = randomUUID();
  const res = await call("/internal/projection/run", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-neon-trigger-invocation-id": invocationId,
    },
    body: JSON.stringify({
      version: 1,
      invocation_id: invocationId,
      trigger: { type: "schedule", id: "t", name: "headwaters-projection" },
      data: { scheduled_at: new Date().toISOString() },
    }),
  });
  assert.equal(res.status, 200, await res.clone().text());
  const run = await res.json();
  assert.equal(typeof run.applied, "number");
  assert.equal(run.caughtUp, true);
});

test("without the trigger header the path is not mounted", async () => {
  const res = await call("/internal/projection/run", { method: "POST" });
  assert.equal(res.status, 404);
});
