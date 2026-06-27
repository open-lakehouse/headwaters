//! The read-API message types, re-exported from [`headwaters_proto`].
//!
//! These are the owned proto messages the [`HeadwatersClient`](crate::HeadwatersClient)
//! returns. Their open-ended `facets` / `data` / `fields` / `events` fields are
//! `google.protobuf.Struct`; [`struct_to_json`] converts one to a
//! [`serde_json::Value`] so callers can read it without depending on buffa.

pub use headwaters_proto::headwaters::read::v1::{
    Dataset, DatasetType, DatasetVersion, DatasetVersionId, EntityId, EntityKind,
    GetAssetStatsRequest, GetColumnLineageRequest, GetDatasetRequest, GetJobRequest,
    GetJobRunsRequest, GetLineageEventStatsRequest, GetLineageRequest, GetRunFacetsRequest,
    GetTagDownstreamRequest, JobDetail, JobType, LineageEdge, LineageGraph, LineageNode,
    ListDatasetVersionsRequest, ListDatasetVersionsResponse, ListDatasetsRequest,
    ListDatasetsResponse, ListEventsRequest, ListEventsResponse, ListJobsRequest, ListJobsResponse,
    ListNamespacesRequest, ListNamespacesResponse, ListRunsResponse, ListTagsRequest,
    ListTagsResponse, Namespace, RunDetail, RunFacetsResponse, RunState, SearchRequest,
    SearchResponse, SearchResult, StatBucket, StatsResponse, Tag, TagPropagation, TaggedField,
};

/// The `google.protobuf.Struct` carried by open-ended fields (`facets`, `data`,
/// schema `fields`, raw `events`).
pub type Struct = buffa_types::google::protobuf::Struct;

/// Convert an open-ended [`Struct`] into a [`serde_json::Value`] (always a JSON
/// object). Use it to read `facets` / `data` / `fields` / `events` without
/// touching buffa types directly.
pub fn struct_to_json(value: &Struct) -> serde_json::Value {
    serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
}
