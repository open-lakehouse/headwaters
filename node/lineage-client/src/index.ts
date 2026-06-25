// @headwaters/lineage-client — the generated ConnectRPC client + types for
// headwaters' read API, plus the transport seam that makes it host-agnostic.
//
// This is the analog of hydrofoil's `@open-lakehouse/uc-client`: a thin package
// whose job is *generated types + a pluggable transport*. `lineage-ui` and the
// scaffold app depend ONLY on this barrel — never on `./gen` internals directly.

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
  getTransport,
  registerTransport,
} from "./transport.js";
