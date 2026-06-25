// TanStack Query hooks over the read client. Each hook is a thin wrapper that
// keys the query by its RPC + arguments and delegates to the injected
// ReadClient, so caching/loading/error state come for free and the data source
// stays swappable (network, fixtures, host).

import type {
  Dataset,
  JobDetail,
  LineageGraph,
  ListDatasetsResponse,
  ListJobsResponse,
  ListRunsResponse,
  SearchResponse,
  StatsResponse,
} from "@headwaters/lineage-client";
import { type UseQueryResult, useQuery } from "@tanstack/react-query";
import { useReadClient } from "./client-context.js";

/** Root key for all lineage read queries, for selective invalidation. */
export const lineageQueryKey = "headwaters-read" as const;

export interface ListPage {
  namespace?: string;
  limit?: number;
  offset?: number;
}

export function useJobs(page: ListPage = {}): UseQueryResult<ListJobsResponse> {
  const client = useReadClient();
  const { namespace = "", limit = 50, offset = 0 } = page;
  return useQuery({
    queryKey: [lineageQueryKey, "jobs", namespace, limit, offset],
    queryFn: () => client.listJobs({ namespace, limit, offset }),
  });
}

export function useDatasets(
  page: ListPage = {},
): UseQueryResult<ListDatasetsResponse> {
  const client = useReadClient();
  const { namespace = "", limit = 50, offset = 0 } = page;
  return useQuery({
    queryKey: [lineageQueryKey, "datasets", namespace, limit, offset],
    queryFn: () => client.listDatasets({ namespace, limit, offset }),
  });
}

export function useJob(
  namespace: string,
  name: string,
): UseQueryResult<JobDetail> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "job", namespace, name],
    queryFn: () => client.getJob({ namespace, name }),
    enabled: Boolean(namespace && name),
  });
}

export function useDataset(
  namespace: string,
  name: string,
): UseQueryResult<Dataset> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "dataset", namespace, name],
    queryFn: () => client.getDataset({ namespace, name }),
    enabled: Boolean(namespace && name),
  });
}

export function useJobRuns(
  namespace: string,
  name: string,
): UseQueryResult<ListRunsResponse> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "job-runs", namespace, name],
    queryFn: () => client.getJobRuns({ namespace, name }),
    enabled: Boolean(namespace && name),
  });
}

export function useLineage(
  nodeId: string,
  depth = 20,
): UseQueryResult<LineageGraph> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "lineage", nodeId, depth],
    queryFn: () => client.getLineage({ nodeId, depth }),
    enabled: Boolean(nodeId),
  });
}

export function useColumnLineage(nodeId: string): UseQueryResult<LineageGraph> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "column-lineage", nodeId],
    queryFn: () => client.getColumnLineage({ nodeId }),
    enabled: Boolean(nodeId),
  });
}

export function useSearch(
  q: string,
  limit = 20,
): UseQueryResult<SearchResponse> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "search", q, limit],
    queryFn: () => client.search({ q, limit }),
    enabled: q.trim().length > 0,
  });
}

export function useLineageEventStats(
  period = "DAY",
  limit = 30,
): UseQueryResult<StatsResponse> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "stats", "lineage-events", period, limit],
    queryFn: () => client.getLineageEventStats({ period, limit }),
  });
}

export function useAssetStats(
  asset: string,
  period = "DAY",
  limit = 30,
): UseQueryResult<StatsResponse> {
  const client = useReadClient();
  return useQuery({
    queryKey: [lineageQueryKey, "stats", "asset", asset, period, limit],
    queryFn: () => client.getAssetStats({ asset, period, limit }),
    enabled: Boolean(asset),
  });
}
