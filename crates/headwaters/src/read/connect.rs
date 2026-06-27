//! ConnectRPC handler for the read API.
//!
//! Implements the generated [`ReadService`] trait (in
//! [`crate::connect_gen::headwaters::read::v1`]) by delegating to the **same**
//! [`LineageStore`] the REST handlers ([`super::http`]) call. This is the seam
//! that lets one binary serve both REST and Connect on one port against one
//! implementation and one model: the store already returns the read-API proto
//! messages, so these handlers carry no conversion — they read the request
//! fields off the zero-copy [`ServiceRequest`] view and return the store's
//! message via [`Response::ok`].
//!
//! Request defaults mirror the REST query-param defaults (limit 100, depth 20,
//! period `day`, stats limit 30) so the two surfaces behave identically: a
//! Connect client that omits `limit` gets the same page size as a REST caller.
//!
//! References generated code under [`crate::connect_gen`]; run `just proto-gen`
//! before the first build on a freshly-checked-out tree if it is missing.

use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};

use super::{LineageStore, ReadError};
use crate::connect_gen::headwaters::read::v1::ReadService;
use crate::proto::headwaters::read::v1 as pb;

/// REST-parity defaults for pagination / traversal knobs absent on the wire
/// (proto3 scalars default to 0 / empty).
const DEFAULT_LIMIT: usize = 100;
const DEFAULT_DEPTH: usize = 20;
const DEFAULT_STATS_LIMIT: usize = 30;
const DEFAULT_PERIOD: &str = "day";

/// `0` (proto3 unset) → the REST default page size.
fn limit_or(n: i32, default: usize) -> usize {
    if n <= 0 { default } else { n as usize }
}

/// Map the read layer's errors onto the Connect error envelope: not-found stays
/// not-found; everything else is an internal error (same split the REST
/// `IntoResponse` makes between 404 and 500).
impl From<ReadError> for ConnectError {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::NotFound(m) => ConnectError::not_found(m),
            ReadError::Query(m) => ConnectError::internal(m),
        }
    }
}

// The generated trait declares `-> impl Future<Output = ServiceResult<impl
// Encodable<T>>>`; implementing it with `async fn -> ServiceResult<T>` returns
// the concrete `T` (which `impl Encodable<T>` admits). That narrowing trips the
// `refining_impl_trait` lint — expected and idiomatic for these handlers.
#[allow(refining_impl_trait)]
impl ReadService for LineageStore {
    async fn list_namespaces(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListNamespacesRequest>,
    ) -> ServiceResult<pb::ListNamespacesResponse> {
        Response::ok(self.namespaces().await?)
    }

    async fn list_jobs(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListJobsRequest>,
    ) -> ServiceResult<pb::ListJobsResponse> {
        // Empty namespace means "all namespaces" (mirrors the REST `/jobs` vs
        // `/namespaces/{ns}/jobs` binding).
        let namespace = (!request.namespace.is_empty()).then(|| request.namespace.to_string());
        let limit = limit_or(request.limit, DEFAULT_LIMIT);
        let offset = request.offset.max(0) as usize;
        Response::ok(self.jobs(namespace.as_deref(), limit, offset).await?)
    }

    async fn get_job(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetJobRequest>,
    ) -> ServiceResult<pb::JobDetail> {
        Response::ok(self.job(request.namespace, request.name).await?)
    }

    async fn get_job_runs(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetJobRunsRequest>,
    ) -> ServiceResult<pb::ListRunsResponse> {
        Response::ok(self.job_runs(request.namespace, request.name).await?)
    }

    async fn list_datasets(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListDatasetsRequest>,
    ) -> ServiceResult<pb::ListDatasetsResponse> {
        let namespace = (!request.namespace.is_empty()).then(|| request.namespace.to_string());
        let limit = limit_or(request.limit, DEFAULT_LIMIT);
        let offset = request.offset.max(0) as usize;
        Response::ok(self.datasets(namespace.as_deref(), limit, offset).await?)
    }

    async fn get_dataset(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetDatasetRequest>,
    ) -> ServiceResult<pb::Dataset> {
        Response::ok(self.dataset(request.namespace, request.name).await?)
    }

    async fn list_dataset_versions(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListDatasetVersionsRequest>,
    ) -> ServiceResult<pb::ListDatasetVersionsResponse> {
        let limit = limit_or(request.limit, DEFAULT_LIMIT);
        let offset = request.offset.max(0) as usize;
        Response::ok(
            self.dataset_versions(request.namespace, request.name, limit, offset)
                .await?,
        )
    }

    async fn search(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::SearchRequest>,
    ) -> ServiceResult<pb::SearchResponse> {
        let limit = limit_or(request.limit, DEFAULT_LIMIT);
        Response::ok(self.search(request.q, limit).await?)
    }

    async fn get_lineage(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetLineageRequest>,
    ) -> ServiceResult<pb::LineageGraph> {
        let depth = limit_or(request.depth, DEFAULT_DEPTH);
        Response::ok(self.lineage(request.node_id, depth).await?)
    }

    async fn get_column_lineage(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetColumnLineageRequest>,
    ) -> ServiceResult<pb::LineageGraph> {
        Response::ok(self.column_lineage(request.node_id).await?)
    }

    async fn list_events(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ListEventsRequest>,
    ) -> ServiceResult<pb::ListEventsResponse> {
        let limit = limit_or(request.limit, DEFAULT_LIMIT);
        let offset = request.offset.max(0) as usize;
        Response::ok(self.events(limit, offset).await?)
    }

    async fn get_run_facets(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetRunFacetsRequest>,
    ) -> ServiceResult<pb::RunFacetsResponse> {
        Response::ok(self.run_facets(request.run_id).await?)
    }

    async fn list_tags(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::ListTagsRequest>,
    ) -> ServiceResult<pb::ListTagsResponse> {
        Response::ok(self.tags().await?)
    }

    async fn get_tag_downstream(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetTagDownstreamRequest>,
    ) -> ServiceResult<pb::TagPropagation> {
        Response::ok(self.tag_downstream(request.tag).await?)
    }

    async fn get_lineage_event_stats(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetLineageEventStatsRequest>,
    ) -> ServiceResult<pb::StatsResponse> {
        let period = if request.period.is_empty() {
            DEFAULT_PERIOD
        } else {
            request.period
        };
        let limit = limit_or(request.limit, DEFAULT_STATS_LIMIT);
        Response::ok(self.stats_lineage_events(period, limit).await?)
    }

    async fn get_asset_stats(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::GetAssetStatsRequest>,
    ) -> ServiceResult<pb::StatsResponse> {
        let period = if request.period.is_empty() {
            DEFAULT_PERIOD
        } else {
            request.period
        };
        let limit = limit_or(request.limit, DEFAULT_STATS_LIMIT);
        Response::ok(self.stats_asset(request.asset, period, limit).await?)
    }
}
