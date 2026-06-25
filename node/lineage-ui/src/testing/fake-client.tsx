// A fake ReadClient + provider stack for Storybook. The data-connected
// components inject their client via ReadClientProvider, so a fake client is all
// it takes to drive them with fixtures — no transport, no network, no MSW.

import type { ReadClient } from "@headwaters/lineage-client";
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

/** Build a fake ReadClient whose methods resolve to fixtures (or hang / reject
 *  to exercise loading / error states). */
export function makeReadClient(mode: FakeMode = "ok"): ReadClient {
  const respond = <T,>(value: T): Promise<T> => {
    if (mode === "loading") return new Promise<T>(() => {}); // never resolves
    if (mode === "error")
      return Promise.reject(new Error("Simulated read error"));
    return Promise.resolve(value);
  };
  const empty = mode === "empty";

  const client = {
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
  };
  return client as unknown as ReadClient;
}

/** Wrap a story in the provider stack with a fake client for the given mode. */
export function withFakeClient(mode: FakeMode = "ok") {
  return function Decorator(Story: () => ReactNode) {
    // A fresh QueryClient per render so mode changes aren't masked by cache.
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: 0 } },
    });
    return (
      <QueryClientProvider client={queryClient}>
        <ReadClientProvider client={makeReadClient(mode)}>
          <Story />
        </ReadClientProvider>
      </QueryClientProvider>
    );
  };
}
