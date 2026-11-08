import { createRouterTransport, type Transport } from "@connectrpc/connect";
import { registerHost, ServiceUnavailableError } from "@open-lakehouse/host";
import { createFakeHost } from "@open-lakehouse/host/testing";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createReadClient } from "./client.js";
import { ReadService } from "./gen/headwaters/read/v1/service_pb.js";
import {
  HEADWATERS_MODULE_ID,
  registerTransport,
  transportFor,
} from "./transport.js";

const LIST_JOBS = "/headwaters.read.v1.ReadService/ListJobs";

/** A transport whose `listJobs` answers with `totalCount`, to tell them apart. */
function serving(totalCount: number): Transport {
  return createRouterTransport(({ service }) =>
    service(ReadService, { listJobs: () => ({ jobs: [], totalCount }) }),
  );
}

const hostServing = (totalCount: number) =>
  createFakeHost({
    services: { [HEADWATERS_MODULE_ID]: { transport: serving(totalCount) } },
  });

afterEach(() => {
  registerHost(null);
  registerTransport(null);
  vi.unstubAllGlobals();
});

describe("clientTransport", () => {
  it("uses a host registered before the client is created", async () => {
    registerHost(hostServing(1));
    const client = createReadClient();
    expect((await client.listJobs({})).totalCount).toBe(1);
  });

  it("uses a host registered after the client is created", async () => {
    const client = createReadClient();
    registerHost(hostServing(2));
    expect((await client.listJobs({})).totalCount).toBe(2);
  });

  it("follows a rebinding without a new client", async () => {
    const host = hostServing(3);
    registerHost(host);
    const client = createReadClient();
    expect((await client.listJobs({})).totalCount).toBe(3);
    host.setService(HEADWATERS_MODULE_ID, { transport: serving(4) });
    expect((await client.listJobs({})).totalCount).toBe(4);
  });

  it("speaks Connect over a binding that only has fetch", async () => {
    const host = createFakeHost({
      services: {
        [HEADWATERS_MODULE_ID]: {
          [`POST ${LIST_JOBS}`]: { jobs: [], totalCount: 5 },
        },
      },
    });
    registerHost(host);
    const client = createReadClient();
    expect((await client.listJobs({})).totalCount).toBe(5);
    expect(new URL(host.requests[0].request.url).pathname).toBe(LIST_JOBS);
  });

  it("prefers registerTransport over the host", async () => {
    registerHost(hostServing(6));
    registerTransport(serving(7));
    const client = createReadClient();
    expect((await client.listJobs({})).totalCount).toBe(7);
    registerTransport(null);
    expect((await client.listJobs({})).totalCount).toBe(6);
  });

  it("rejects when the host doesn't bind headwaters", async () => {
    registerHost(createFakeHost());
    const client = createReadClient();
    const call = client.listJobs({});
    await expect(call).rejects.toBeInstanceOf(ServiceUnavailableError);
    await expect(call).rejects.toMatchObject({
      moduleId: HEADWATERS_MODULE_ID,
    });
  });

  it("calls the page's own origin without a host", async () => {
    const fetch = vi.fn(async () => Response.json({ jobs: [], totalCount: 8 }));
    vi.stubGlobal("fetch", fetch);
    const client = createReadClient();
    expect((await client.listJobs({})).totalCount).toBe(8);
    expect(fetch.mock.calls[0]).toEqual([LIST_JOBS, expect.anything()]);
  });
});

describe("transportFor", () => {
  it("uses the given host over the registered one", async () => {
    registerHost(hostServing(9));
    const client = createReadClient(transportFor(hostServing(10)));
    expect((await client.listJobs({})).totalCount).toBe(10);
  });
});
