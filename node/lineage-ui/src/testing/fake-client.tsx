// A fake `@open-lakehouse/host` host for Storybook and tests. It binds
// `headwaters` to an in-memory ConnectRPC router that answers from the
// fixtures, so a story takes the same path as the desktop shell:
// <HostProvider> → <ReadClientProvider> (no client given) → the host binding →
// the read API. No network and no MSW.

import { Code, ConnectError, createRouterTransport } from "@connectrpc/connect";
import { HostProvider } from "@open-lakehouse/host/react";
import { createFakeHost, type FakeHost } from "@open-lakehouse/host/testing";
import {
  HEADWATERS_MODULE_ID,
  ReadService,
} from "@open-lakehouse/lineage-client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { ReadClientProvider } from "../hooks/client-context.js";
import {
  mockColumnGraph,
  mockDataset,
  mockJob,
  mockRun,
  mockSearchResults,
  mockStatBuckets,
  mockTableGraph,
} from "./mock-data.js";

export type FakeMode = "ok" | "loading" | "error" | "empty";

/** A read-API router answering from the fixtures (or hanging / failing, to
 *  exercise loading and error states). */
export function fakeReadTransport(mode: FakeMode = "ok") {
  const respond = <T,>(value: T): Promise<T> => {
    if (mode === "loading") return new Promise<T>(() => {}); // never resolves
    if (mode === "error") {
      return Promise.reject(
        new ConnectError("Simulated read error", Code.Unavailable),
      );
    }
    return Promise.resolve(value);
  };
  const empty = mode === "empty";

  return createRouterTransport(({ service }) =>
    service(ReadService, {
      listJobs: () =>
        respond({ jobs: empty ? [] : [mockJob], totalCount: empty ? 0 : 1 }),
      listDatasets: () =>
        respond({
          datasets: empty ? [] : [mockDataset],
          totalCount: empty ? 0 : 1,
        }),
      getJob: () => respond(mockJob),
      getDataset: () => respond(mockDataset),
      getJobRuns: () =>
        respond({ runs: empty ? [] : [mockRun], totalCount: empty ? 0 : 1 }),
      getLineage: () => respond(empty ? { graph: [] } : mockTableGraph),
      getColumnLineage: () => respond(empty ? { graph: [] } : mockColumnGraph),
      search: () =>
        respond({
          results: empty ? [] : mockSearchResults,
          totalCount: empty ? 0 : 3,
        }),
      getLineageEventStats: () =>
        respond({ buckets: empty ? [] : mockStatBuckets }),
      getAssetStats: () => respond({ buckets: empty ? [] : mockStatBuckets }),
      listNamespaces: () => respond({ namespaces: [] }),
      listDatasetVersions: () => respond({ versions: [], totalCount: 0 }),
      listEvents: () => respond({ events: [], totalCount: 0 }),
      getRunFacets: () => respond({ runId: "", facets: {} }),
      listTags: () => respond({ tags: [] }),
      getTagDownstream: () => respond({ tag: "", fields: [] }),
    }),
  );
}

/** A fake host whose `headwaters` binding answers from the fixtures. */
export function fakeLineageHost(mode: FakeMode = "ok"): FakeHost {
  return createFakeHost({
    services: {
      [HEADWATERS_MODULE_ID]: { transport: fakeReadTransport(mode) },
    },
  });
}

/** Wrap a story in a fake host and the provider stack, for the given mode. */
export function withFakeClient(mode: FakeMode = "ok") {
  return function Decorator(Story: () => ReactNode) {
    // A fresh QueryClient per render so mode changes aren't masked by cache.
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: 0 } },
    });
    return (
      <HostProvider host={fakeLineageHost(mode)}>
        <QueryClientProvider client={queryClient}>
          <ReadClientProvider>
            <Story />
          </ReadClientProvider>
        </QueryClientProvider>
      </HostProvider>
    );
  };
}
