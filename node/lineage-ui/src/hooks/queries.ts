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
} from "@open-lakehouse/lineage-client";
import { type UseQueryResult, useQuery } from "@tanstack/react-query";
import { useReadClient, useReadScope } from "./client-context.js";

/**
 * Root key for all lineage read queries, for selective invalidation. Every key
 * is `[lineageQueryKey, scope, …]`, the scope being the provider's (the host's
 * environment key by default), so `[lineageQueryKey]` matches them all.
 */
export const lineageQueryKey = "headwaters-read" as const;

export interface ListPage {
  namespace?: string;
  limit?: number;
  offset?: number;
}

export function useJobs(page: ListPage = {}): UseQueryResult<ListJobsResponse> {
  const client = useReadClient();
  const scope = useReadScope();
  const { namespace = "", limit = 50, offset = 0 } = page;
  return useQuery({
    queryKey: [lineageQueryKey, scope, "jobs", namespace, limit, offset],
    queryFn: () => client.listJobs({ namespace, limit, offset }),
  });
}

export function useDatasets(
  page: ListPage = {},
): UseQueryResult<ListDatasetsResponse> {
  const client = useReadClient();
  const scope = useReadScope();
  const { namespace = "", limit = 50, offset = 0 } = page;
  return useQuery({
    queryKey: [lineageQueryKey, scope, "datasets", namespace, limit, offset],
    queryFn: () => client.listDatasets({ namespace, limit, offset }),
  });
}

export function useJob(
  namespace: string,
  name: string,
): UseQueryResult<JobDetail> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [lineageQueryKey, scope, "job", namespace, name],
    queryFn: () => client.getJob({ namespace, name }),
    enabled: Boolean(namespace && name),
  });
}

export function useDataset(
  namespace: string,
  name: string,
): UseQueryResult<Dataset> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [lineageQueryKey, scope, "dataset", namespace, name],
    queryFn: () => client.getDataset({ namespace, name }),
    enabled: Boolean(namespace && name),
  });
}

export function useJobRuns(
  namespace: string,
  name: string,
): UseQueryResult<ListRunsResponse> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [lineageQueryKey, scope, "job-runs", namespace, name],
    queryFn: () => client.getJobRuns({ namespace, name }),
    enabled: Boolean(namespace && name),
  });
}

export function useLineage(
  nodeId: string,
  depth = 20,
): UseQueryResult<LineageGraph> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [lineageQueryKey, scope, "lineage", nodeId, depth],
    queryFn: () => client.getLineage({ nodeId, depth }),
    enabled: Boolean(nodeId),
  });
}

export function useColumnLineage(nodeId: string): UseQueryResult<LineageGraph> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [lineageQueryKey, scope, "column-lineage", nodeId],
    queryFn: () => client.getColumnLineage({ nodeId }),
    enabled: Boolean(nodeId),
  });
}

export function useSearch(
  q: string,
  limit = 20,
): UseQueryResult<SearchResponse> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [lineageQueryKey, scope, "search", q, limit],
    queryFn: () => client.search({ q, limit }),
    enabled: q.trim().length > 0,
  });
}

export function useLineageEventStats(
  period = "DAY",
  limit = 30,
): UseQueryResult<StatsResponse> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [
      lineageQueryKey,
      scope,
      "stats",
      "lineage-events",
      period,
      limit,
    ],
    queryFn: () => client.getLineageEventStats({ period, limit }),
  });
}

export function useAssetStats(
  asset: string,
  period = "DAY",
  limit = 30,
): UseQueryResult<StatsResponse> {
  const client = useReadClient();
  const scope = useReadScope();
  return useQuery({
    queryKey: [lineageQueryKey, scope, "stats", "asset", asset, period, limit],
    queryFn: () => client.getAssetStats({ asset, period, limit }),
    enabled: Boolean(asset),
  });
}
