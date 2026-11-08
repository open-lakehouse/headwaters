// @vitest-environment happy-dom

import { createRouterTransport } from "@connectrpc/connect";
import { registerHost } from "@open-lakehouse/host";
import { HostProvider } from "@open-lakehouse/host/react";
import { createFakeHost } from "@open-lakehouse/host/testing";
import {
  HEADWATERS_MODULE_ID,
  ReadService,
  registerTransport,
} from "@open-lakehouse/lineage-client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { ReadClientProvider, useReadScope } from "./client-context.js";
import { useJobs } from "./queries.js";

/** A headwaters binding whose `listJobs` reports `totalCount`. */
const binding = (totalCount: number) => ({
  transport: createRouterTransport(({ service }) =>
    service(ReadService, { listJobs: () => ({ jobs: [], totalCount }) }),
  ),
});

function Jobs() {
  const { data } = useJobs();
  const scope = useReadScope();
  return (
    <p>
      {scope}: {data ? `${data.totalCount} jobs` : "loading"}
    </p>
  );
}

const withQueries = (children: ReactNode) => (
  <QueryClientProvider
    client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}
  >
    {children}
  </QueryClientProvider>
);

afterEach(() => {
  cleanup();
  registerHost(null);
  registerTransport(null);
});

describe("ReadClientProvider", () => {
  it("reads through a provided host's headwaters binding", async () => {
    const host = createFakeHost({
      services: { [HEADWATERS_MODULE_ID]: binding(3) },
    });
    render(
      <HostProvider host={host}>
        {withQueries(
          <ReadClientProvider>
            <Jobs />
          </ReadClientProvider>,
        )}
      </HostProvider>,
    );
    expect(await screen.findByText("dev#0: 3 jobs")).toBeTruthy();
  });

  it("refetches under a new scope when the environment switches", async () => {
    const host = createFakeHost({
      services: { [HEADWATERS_MODULE_ID]: binding(1) },
    });
    registerHost(host);
    render(
      withQueries(
        <ReadClientProvider>
          <Jobs />
        </ReadClientProvider>,
      ),
    );
    expect(await screen.findByText("dev#0: 1 jobs")).toBeTruthy();

    // setService bumps the key once; setEnvironment bumps it again.
    act(() => {
      host.setService(HEADWATERS_MODULE_ID, binding(2));
      host.setEnvironment({ id: "staging", name: "staging" });
    });
    expect(await screen.findByText("staging#2: 2 jobs")).toBeTruthy();
  });

  it("lets registerTransport win over the host", async () => {
    registerHost(
      createFakeHost({ services: { [HEADWATERS_MODULE_ID]: binding(1) } }),
    );
    registerTransport(binding(9).transport);
    render(
      withQueries(
        <ReadClientProvider scope="pinned">
          <Jobs />
        </ReadClientProvider>,
      ),
    );
    expect(await screen.findByText("pinned: 9 jobs")).toBeTruthy();
  });
});
