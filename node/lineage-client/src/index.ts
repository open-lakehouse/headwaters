// @open-lakehouse/lineage-client — the generated ConnectRPC client + types for
// headwaters' read API. Calls reach headwaters through the `@open-lakehouse/host`
// binding of whatever runs the UI, or through `registerTransport` (see
// transport.ts). `lineage-ui` and the scaffold app depend ONLY on this barrel —
// never on `./gen` internals directly.

// The host's error for a service the current environment doesn't bind.
export { ServiceUnavailableError } from "@open-lakehouse/host";
// --- client + transport seam ---
export { createReadClient, type ReadClient } from "./client.js";
// --- read-API message types (the read model the UI renders) ---
export type {
  Dataset,
  DatasetVersion,
  DatasetVersionId,
  EntityId,
  JobDetail,
  LineageEdge,
  LineageGraph,
  LineageNode,
  ListDatasetsResponse,
  ListDatasetVersionsResponse,
  ListEventsResponse,
  ListJobsResponse,
  ListNamespacesResponse,
  ListRunsResponse,
  ListTagsResponse,
  Namespace,
  RunDetail,
  RunFacetsResponse,
  SearchResponse,
  SearchResult,
  StatBucket,
  StatsResponse,
  Tag,
  TaggedField,
  TagPropagation,
} from "./gen/headwaters/read/v1/read_pb.js";
// --- request message types (for typed call sites / fixtures) ---
export type {
  GetAssetStatsRequest,
  GetColumnLineageRequest,
  GetDatasetRequest,
  GetJobRequest,
  GetJobRunsRequest,
  GetLineageEventStatsRequest,
  GetLineageRequest,
  GetRunFacetsRequest,
  GetTagDownstreamRequest,
  ListDatasetsRequest,
  ListDatasetVersionsRequest,
  ListEventsRequest,
  ListJobsRequest,
  ListNamespacesRequest,
  SearchRequest,
} from "./gen/headwaters/read/v1/service_pb.js";
// --- service descriptor (for hosts building their own client / fixtures) ---
export { ReadService } from "./gen/headwaters/read/v1/service_pb.js";
export {
  clientTransport,
  createDefaultTransport,
  getTransport,
  HEADWATERS_MODULE_ID,
  registerTransport,
  transportFor,
} from "./transport.js";
