// @headwaters/lineage-ui — composable, host-agnostic React components for the
// headwaters lineage read API. This barrel is the integration surface: the
// scaffold app and any host (e.g. hydrofoil) import ONLY from here, never from
// internal paths. See README.md for the public/internal contract.

// --- re-export the read-model types so hosts need only one dependency ---
export type {
  Dataset,
  EntityId,
  JobDetail,
  LineageGraph,
  LineageNode,
  RunDetail,
  SearchResult,
  StatBucket,
} from "@headwaters/lineage-client";
export type { DatasetBrowserProps } from "./browser/DatasetBrowser.js";
// --- browsers ---
export { DatasetBrowser } from "./browser/DatasetBrowser.js";
export type { JobBrowserProps } from "./browser/JobBrowser.js";
export { JobBrowser } from "./browser/JobBrowser.js";
// --- shared primitives (useful for hosts composing their own layouts) ---
export { AsyncBoundary } from "./components/ui/AsyncBoundary.js";
export { Pager } from "./components/ui/Pager.js";
export { RunStateBadge } from "./components/ui/RunStateBadge.js";
export type { ReadClientProviderProps } from "./hooks/client-context.js";
// --- client injection (the React companion to lineage-client's transport seam) ---
export { ReadClientProvider, useReadClient } from "./hooks/client-context.js";
export type { ListPage } from "./hooks/queries.js";
// --- data hooks (TanStack Query over the read client) ---
export {
  lineageQueryKey,
  useAssetStats,
  useColumnLineage,
  useDataset,
  useDatasets,
  useJob,
  useJobRuns,
  useJobs,
  useLineage,
  useLineageEventStats,
  useSearch,
} from "./hooks/queries.js";
