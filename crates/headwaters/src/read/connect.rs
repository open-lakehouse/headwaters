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
        // `ENTITY_KIND_UNSPECIFIED` (and any field-level node kind) means "no
        // filter" — only an explicit JOB/DATASET narrows the search.
        let kind = match request.r#type.as_known() {
            Some(k @ (pb::EntityKind::JOB | pb::EntityKind::DATASET)) => Some(k),
            _ => None,
        };
        let namespace = (!request.namespace.is_empty()).then_some(request.namespace);
        Response::ok(self.search(request.q, limit, kind, namespace).await?)
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

#[cfg(test)]
mod limit_tests {
    use super::*;

    #[test]
    fn limit_or_maps_unset_and_negative_to_default() {
        // proto3 scalars default to 0 on the wire; 0 and anything ≤ 0 means
        // "unset" and must fall back to the REST-parity default.
        assert_eq!(limit_or(0, 100), 100);
        assert_eq!(limit_or(-5, 100), 100);
    }

    #[test]
    fn limit_or_passes_through_positive() {
        assert_eq!(limit_or(25, 100), 25);
        assert_eq!(limit_or(1, 100), 1);
    }

    #[test]
    fn read_error_maps_onto_connect_error_codes() {
        use connectrpc::ErrorCode;

        let nf: ConnectError = ReadError::NotFound("missing".into()).into();
        assert_eq!(nf.code, ErrorCode::NotFound);

        let internal: ConnectError = ReadError::Query("boom".into()).into();
        assert_eq!(internal.code, ErrorCode::Internal);
    }
}

// End-to-end ConnectRPC handler tests. These run *inside* the crate because the
// handlers and their request/response types (`crate::proto`, `crate::connect_gen`)
// are crate-private — an external `tests/` integration crate cannot reach them.
// Postgres-gated like the REST tests in `tests/read_test.rs`; they share the same
// seed helpers via `crate::test_support`.
#[cfg(all(test, feature = "postgres-it"))]
mod handler_tests {
    use super::*;
    use crate::test_support::{
        column_lineage_seeded_store, seeded_store, start_postgres, uri_seeded_store,
    };
    use buffa::Message;
    use buffa::view::MessageView;
    use bytes::Bytes;
    use connectrpc::{ErrorCode, RequestContext};

    /// Invoke a `ReadService` handler with a request built from an owned proto
    /// message. The encode→decode-view→`from_parts` dance has to happen in the
    /// caller's scope so the `Bytes` and view outlive the borrow the
    /// `ServiceRequest` holds; a macro keeps those bindings local.
    macro_rules! call {
        ($store:expr, $method:ident, $req:expr, $view:ty) => {{
            let bytes = Bytes::from($req.encode_to_vec());
            let view = <$view>::decode_view(&bytes).expect("decode view");
            let request = ServiceRequest::from_parts(&view, &bytes);
            // Fully-qualified trait call: `search` collides with the inherent
            // `LineageStore::search`, so dispatch through `ReadService` explicitly
            // for every method.
            ReadService::$method(&$store, RequestContext::default(), request).await
        }};
    }

    #[tokio::test]
    async fn list_jobs_unset_limit_uses_rest_default() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        // limit/offset left at proto3 zero — the handler must apply DEFAULT_LIMIT,
        // not request a zero-row page.
        let resp = call!(
            store,
            list_jobs,
            pb::ListJobsRequest {
                namespace: "etl".into(),
                ..Default::default()
            },
            pb::ListJobsRequestView
        )
        .expect("list_jobs ok");
        assert_eq!(resp.body.total_count, 1);
        assert_eq!(resp.body.jobs[0].name, "build_daily");
    }

    #[tokio::test]
    async fn list_jobs_empty_namespace_means_all() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        let resp = call!(
            store,
            list_jobs,
            pb::ListJobsRequest::default(),
            pb::ListJobsRequestView
        )
        .expect("list_jobs ok");
        assert_eq!(resp.body.total_count, 1, "empty namespace lists all jobs");
    }

    #[tokio::test]
    async fn get_job_returns_inputs_and_outputs() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        let resp = call!(
            store,
            get_job,
            pb::GetJobRequest {
                namespace: "etl".into(),
                name: "build_daily".into(),
                ..Default::default()
            },
            pb::GetJobRequestView
        )
        .expect("get_job ok");
        assert_eq!(resp.body.inputs[0].name, "orders");
        assert_eq!(resp.body.outputs[0].name, "daily_orders");
    }

    #[tokio::test]
    async fn get_job_unknown_maps_to_connect_not_found() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        let err = call!(
            store,
            get_job,
            pb::GetJobRequest {
                namespace: "etl".into(),
                name: "nope".into(),
                ..Default::default()
            },
            pb::GetJobRequestView
        )
        .expect_err("missing job is an error");
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn list_namespaces_serves_seeded_namespaces() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        let resp = call!(
            store,
            list_namespaces,
            pb::ListNamespacesRequest::default(),
            pb::ListNamespacesRequestView
        )
        .expect("list_namespaces ok");
        let names: Vec<&str> = resp
            .body
            .namespaces
            .iter()
            .map(|n| n.name.as_str())
            .collect();
        assert!(names.contains(&"etl"), "namespaces: {names:?}");
    }

    #[tokio::test]
    async fn search_unset_limit_uses_default_and_finds_matches() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        let resp = call!(
            store,
            search,
            pb::SearchRequest {
                q: "orders".into(),
                ..Default::default()
            },
            pb::SearchRequestView
        )
        .expect("search ok");
        assert!(resp.body.total_count >= 2, "got {}", resp.body.total_count);
    }

    #[tokio::test]
    async fn get_lineage_unset_depth_resolves_graph() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        // depth left at 0 (proto3 unset) must fall back to DEFAULT_DEPTH and still
        // walk out to the connected datasets.
        let resp = call!(
            store,
            get_lineage,
            pb::GetLineageRequest {
                node_id: "job:etl:build_daily".into(),
                ..Default::default()
            },
            pb::GetLineageRequestView
        )
        .expect("get_lineage ok");
        let ids: Vec<&str> = resp.body.graph.iter().map(|n| n.id.as_str()).collect();
        assert!(ids.contains(&"dataset:raw:orders"), "graph: {ids:?}");
        assert!(
            ids.contains(&"dataset:marts:daily_orders"),
            "graph: {ids:?}"
        );
    }

    #[tokio::test]
    async fn get_lineage_unknown_seed_maps_to_not_found() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        let err = call!(
            store,
            get_lineage,
            pb::GetLineageRequest {
                node_id: "dataset:nope:missing".into(),
                ..Default::default()
            },
            pb::GetLineageRequestView
        )
        .expect_err("unknown seed is an error");
        assert_eq!(err.code, ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn get_run_facets_merges_run_facets() {
        let db = start_postgres().await;
        let store = uri_seeded_store(&db).await;
        let resp = call!(
            store,
            get_run_facets,
            pb::GetRunFacetsRequest {
                run_id: "r1".into(),
                ..Default::default()
            },
            pb::GetRunFacetsRequestView
        )
        .expect("get_run_facets ok");
        assert_eq!(resp.body.run_id, "r1");
    }

    #[tokio::test]
    async fn get_column_lineage_serves_field_graph() {
        let db = start_postgres().await;
        let store = column_lineage_seeded_store(&db).await;
        let resp = call!(
            store,
            get_column_lineage,
            pb::GetColumnLineageRequest {
                node_id: "dataset:warehouse:silver.customers".into(),
                ..Default::default()
            },
            pb::GetColumnLineageRequestView
        )
        .expect("get_column_lineage ok");
        let ids: Vec<&str> = resp.body.graph.iter().map(|n| n.id.as_str()).collect();
        assert!(
            ids.contains(&"datasetField:warehouse:silver.customers:id"),
            "field graph: {ids:?}"
        );
        // The latest facet wins: `id` maps from customer_key, not the older `id`.
        assert!(
            ids.contains(&"datasetField:raw:customers:customer_key"),
            "latest mapping: {ids:?}"
        );
    }

    #[tokio::test]
    async fn get_lineage_event_stats_empty_period_uses_default() {
        let db = start_postgres().await;
        let store = seeded_store(&db).await;
        // Empty period string (proto3 unset) must fall back to DEFAULT_PERIOD
        // ("day") rather than passing an empty bucket spec to the query layer.
        let resp = call!(
            store,
            get_lineage_event_stats,
            pb::GetLineageEventStatsRequest::default(),
            pb::GetLineageEventStatsRequestView
        )
        .expect("get_lineage_event_stats ok");
        // One COMPLETE event was seeded; it lands in a single day bucket.
        let total: i64 = resp.body.buckets.iter().map(|b| b.count).sum();
        assert_eq!(total, 1);
    }
}
