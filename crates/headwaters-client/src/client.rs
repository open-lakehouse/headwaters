//! The [`HeadwatersClient`] and its builder.

use connectrpc::client::{ClientConfig, ClientTransport, HttpClient};
use headwaters_proto::connect_gen::headwaters::read::v1::ReadServiceClient;

use crate::types::*;
use crate::{Error, HeadwatersConfig};

/// An async client for the Headwaters read API.
///
/// Construct one from a base URL with [`connect`](Self::connect), from the
/// environment with [`from_env`](Self::from_env), or from a prepared
/// [`HeadwatersConfig`] with [`new`](Self::new). Each method maps to one read
/// RPC and returns an owned response message; the open-ended `facets` / `data`
/// fields on those messages are `google.protobuf.Struct`, which you can turn
/// into a [`serde_json::Value`] with [`struct_to_json`].
///
/// # Examples
///
/// ```no_run
/// # async fn run() -> Result<(), headwaters_client::Error> {
/// use headwaters_client::HeadwatersClient;
///
/// let client = HeadwatersClient::connect("http://localhost:8091")?;
/// let namespaces = client.list_namespaces().await?;
/// for ns in &namespaces.namespaces {
///     println!("{}", ns.name);
/// }
/// # Ok(())
/// # }
/// ```
///
/// The transport `T` defaults to [`HttpClient`] (plaintext/TLS over hyper),
/// which is what [`connect`](Self::connect), [`new`](Self::new), and
/// [`from_env`](Self::from_env) build. With the `cloud-auth` feature, the
/// cloud constructors return a client over olai-http's `CloudTransport`, which
/// signs each request for a cloud provider (AWS/Azure/GCP/Databricks).
#[derive(Clone)]
pub struct HeadwatersClient<T = HttpClient>
where
    T: ClientTransport,
{
    inner: ReadServiceClient<T>,
}

impl HeadwatersClient<HttpClient> {
    /// Connect to a server at `base_url` (e.g. `http://localhost:8091`) with
    /// default settings. Shorthand for `new(HeadwatersConfig::new(base_url))`.
    pub fn connect(base_url: impl Into<String>) -> Result<Self, Error> {
        Self::new(HeadwatersConfig::new(base_url))
    }

    /// Build a client from a [`HeadwatersConfig`].
    ///
    /// Only `http://` base URLs are supported today; an `https://` URL returns
    /// [`Error::InvalidBaseUrl`] (TLS support is a planned follow-up).
    pub fn new(config: HeadwatersConfig) -> Result<Self, Error> {
        let uri: http::Uri = config
            .base_url
            .parse()
            .map_err(|_| Error::InvalidBaseUrl(config.base_url.clone()))?;
        if uri.scheme_str() == Some("https") {
            return Err(Error::InvalidBaseUrl(format!(
                "{}: https is not supported yet (TLS is a planned follow-up)",
                config.base_url
            )));
        }

        let mut client_config = ClientConfig::new(uri).with_default_timeout(config.timeout);
        if let Some(token) = &config.token {
            client_config =
                client_config.with_default_header("authorization", format!("Bearer {token}"));
        }

        Ok(Self {
            inner: ReadServiceClient::new(HttpClient::plaintext(), client_config),
        })
    }

    /// Build a client from the environment (`HEADWATERS_URL`, `HEADWATERS_TOKEN`,
    /// `HEADWATERS_TIMEOUT_MS`). See [`HeadwatersConfig::from_env`].
    pub fn from_env() -> Result<Self, Error> {
        Self::new(HeadwatersConfig::from_env()?)
    }
}

impl<T> HeadwatersClient<T>
where
    T: ClientTransport,
    <T::ResponseBody as http_body::Body>::Error: std::fmt::Display,
{
    // --- namespaces ---

    /// List every namespace.
    pub async fn list_namespaces(&self) -> Result<ListNamespacesResponse, Error> {
        Ok(self
            .inner
            .list_namespaces(ListNamespacesRequest::default())
            .await?
            .into_owned())
    }

    // --- jobs ---

    /// List jobs, optionally scoped to one namespace (`""` = all).
    pub async fn list_jobs(
        &self,
        namespace: &str,
        limit: i32,
        offset: i32,
    ) -> Result<ListJobsResponse, Error> {
        Ok(self
            .inner
            .list_jobs(ListJobsRequest {
                namespace: namespace.to_string(),
                limit,
                offset,
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// Get one job by namespace and name.
    pub async fn get_job(&self, namespace: &str, name: &str) -> Result<JobDetail, Error> {
        Ok(self
            .inner
            .get_job(GetJobRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// List the runs of one job.
    pub async fn get_job_runs(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<ListRunsResponse, Error> {
        Ok(self
            .inner
            .get_job_runs(GetJobRunsRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    // --- datasets ---

    /// List datasets, optionally scoped to one namespace (`""` = all).
    pub async fn list_datasets(
        &self,
        namespace: &str,
        limit: i32,
        offset: i32,
    ) -> Result<ListDatasetsResponse, Error> {
        Ok(self
            .inner
            .list_datasets(ListDatasetsRequest {
                namespace: namespace.to_string(),
                limit,
                offset,
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// Get one dataset by namespace and name.
    pub async fn get_dataset(&self, namespace: &str, name: &str) -> Result<Dataset, Error> {
        Ok(self
            .inner
            .get_dataset(GetDatasetRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// List the schema versions of one dataset, newest first.
    pub async fn list_dataset_versions(
        &self,
        namespace: &str,
        name: &str,
        limit: i32,
        offset: i32,
    ) -> Result<ListDatasetVersionsResponse, Error> {
        Ok(self
            .inner
            .list_dataset_versions(ListDatasetVersionsRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                limit,
                offset,
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    // --- search / graph / events / facets ---

    /// Substring search over job and dataset names. `kind` restricts to one
    /// entity kind (`ENTITY_KIND_UNSPECIFIED` = both); `namespace` restricts to
    /// one namespace (`""` = all).
    pub async fn search(
        &self,
        q: &str,
        limit: i32,
        kind: EntityKind,
        namespace: &str,
    ) -> Result<SearchResponse, Error> {
        Ok(self
            .inner
            .search(SearchRequest {
                q: q.to_string(),
                limit,
                r#type: kind.into(),
                namespace: namespace.to_string(),
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// Entity-level lineage: the jobs and datasets within `depth` hops of the
    /// seed `node_id`, with the edges between them.
    pub async fn get_lineage(&self, node_id: &str, depth: i32) -> Result<LineageGraph, Error> {
        Ok(self
            .inner
            .get_lineage(GetLineageRequest {
                node_id: node_id.to_string(),
                depth,
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// Column-level lineage for the seed `node_id` (a `dataset:` or
    /// `datasetField:` nodeId).
    pub async fn get_column_lineage(&self, node_id: &str) -> Result<LineageGraph, Error> {
        Ok(self
            .inner
            .get_column_lineage(GetColumnLineageRequest {
                node_id: node_id.to_string(),
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// The raw OpenLineage events feed, newest first.
    pub async fn list_events(&self, limit: i32, offset: i32) -> Result<ListEventsResponse, Error> {
        Ok(self
            .inner
            .list_events(ListEventsRequest {
                limit,
                offset,
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// The merged facets observed across all of a run's events.
    pub async fn get_run_facets(&self, run_id: &str) -> Result<RunFacetsResponse, Error> {
        Ok(self
            .inner
            .get_run_facets(GetRunFacetsRequest {
                run_id: run_id.to_string(),
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    // --- tags ---

    /// List every tag in the catalog.
    pub async fn list_tags(&self) -> Result<ListTagsResponse, Error> {
        Ok(self
            .inner
            .list_tags(ListTagsRequest::default())
            .await?
            .into_owned())
    }

    /// Every field a tag reaches downstream through column lineage.
    pub async fn get_tag_downstream(&self, tag: &str) -> Result<TagPropagation, Error> {
        Ok(self
            .inner
            .get_tag_downstream(GetTagDownstreamRequest {
                tag: tag.to_string(),
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    // --- stats ---

    /// Time-bucketed counts of lineage events. `period` is `HOUR` | `DAY` |
    /// `WEEK` | `MONTH`; `limit` caps the number of buckets.
    pub async fn get_lineage_event_stats(
        &self,
        period: &str,
        limit: i32,
    ) -> Result<StatsResponse, Error> {
        Ok(self
            .inner
            .get_lineage_event_stats(GetLineageEventStatsRequest {
                period: period.to_string(),
                limit,
                ..Default::default()
            })
            .await?
            .into_owned())
    }

    /// Time-bucketed first-seen counts of an asset kind (`jobs` | `datasets`).
    pub async fn get_asset_stats(
        &self,
        asset: &str,
        period: &str,
        limit: i32,
    ) -> Result<StatsResponse, Error> {
        Ok(self
            .inner
            .get_asset_stats(GetAssetStatsRequest {
                asset: asset.to_string(),
                period: period.to_string(),
                limit,
                ..Default::default()
            })
            .await?
            .into_owned())
    }
}

#[cfg(feature = "cloud-auth")]
mod cloud {
    use connectrpc::client::ClientConfig;
    use headwaters_proto::connect_gen::headwaters::read::v1::ReadServiceClient;
    use olai_http::CloudClient;
    use olai_http::connectrpc::CloudTransport;

    use super::HeadwatersClient;
    use crate::{Error, HeadwatersConfig};

    /// Build a [`ClientConfig`] for a cloud-fronted server. Unlike the bearer
    /// path, this sets no `authorization` default header — the [`CloudClient`]
    /// signer owns request authentication.
    fn cloud_config(config: &HeadwatersConfig) -> Result<ClientConfig, Error> {
        let uri: http::Uri = config
            .base_url
            .parse()
            .map_err(|_| Error::InvalidBaseUrl(config.base_url.clone()))?;
        Ok(ClientConfig::new(uri).with_default_timeout(config.timeout))
    }

    impl HeadwatersClient<CloudTransport> {
        /// Build a client that authenticates with a pre-built [`CloudClient`].
        ///
        /// Use this to bring your own credential configuration (e.g. an
        /// [`CloudClient`] constructed with a specific runtime handle or a
        /// custom credential provider). For the common cases, prefer the
        /// per-provider constructors
        /// ([`new_aws`](Self::new_aws), [`new_azure`](Self::new_azure),
        /// [`new_google`](Self::new_google),
        /// [`new_databricks`](Self::new_databricks)).
        ///
        /// `https://` base URLs are expected here — TLS is handled by the
        /// underlying olai-http reqwest client.
        pub fn new_with_cloud(
            config: HeadwatersConfig,
            client: CloudClient,
        ) -> Result<Self, Error> {
            let client_config = cloud_config(&config)?;
            Ok(Self {
                inner: ReadServiceClient::new(CloudTransport::new(client), client_config),
            })
        }

        /// Authenticate with AWS credentials (SigV4). `options` are olai-http
        /// AWS config keys (see [`olai_http::aws`]); pass an empty iterator to
        /// rely on the ambient environment / instance metadata.
        pub fn new_aws<I, K, V>(config: HeadwatersConfig, options: I) -> Result<Self, Error>
        where
            I: IntoIterator<Item = (K, V)>,
            K: AsRef<str>,
            V: Into<String>,
        {
            Self::new_with_cloud(config, CloudClient::new_aws(options, None)?)
        }

        /// Authenticate with Azure AD credentials. `options` are olai-http Azure
        /// config keys (see [`olai_http::azure`]).
        pub fn new_azure<I, K, V>(config: HeadwatersConfig, options: I) -> Result<Self, Error>
        where
            I: IntoIterator<Item = (K, V)>,
            K: AsRef<str>,
            V: Into<String>,
        {
            Self::new_with_cloud(config, CloudClient::new_azure(options, None)?)
        }

        /// Authenticate with Google Cloud credentials. `options` are olai-http
        /// GCP config keys (see [`olai_http::gcp`]).
        pub fn new_google<I, K, V>(config: HeadwatersConfig, options: I) -> Result<Self, Error>
        where
            I: IntoIterator<Item = (K, V)>,
            K: AsRef<str>,
            V: Into<String>,
        {
            Self::new_with_cloud(config, CloudClient::new_google(options, None)?)
        }

        /// Authenticate with Databricks credentials (OAuth M2M / OIDC / PAT).
        /// `options` are olai-http Databricks config keys (see
        /// [`olai_http::databricks`]).
        pub fn new_databricks<I, K, V>(config: HeadwatersConfig, options: I) -> Result<Self, Error>
        where
            I: IntoIterator<Item = (K, V)>,
            K: AsRef<str>,
            V: Into<String>,
        {
            Self::new_with_cloud(config, CloudClient::new_databricks(options, None)?)
        }

        /// Build a cloud-authenticated client from the environment.
        ///
        /// Reads the base URL and timeout from the usual variables
        /// ([`HeadwatersConfig::from_env`]) and selects the provider from
        /// [`HEADWATERS_AUTH`](crate::ENV_AUTH) (`aws` | `azure` | `gcp` |
        /// `databricks`). Each provider discovers its own credentials from the
        /// ambient environment, so no per-provider options are passed here.
        ///
        /// Returns [`Error::InvalidBaseUrl`] if `HEADWATERS_AUTH` is unset or
        /// names an unknown provider.
        pub fn from_env() -> Result<Self, Error> {
            let config = HeadwatersConfig::from_env()?;
            let provider = std::env::var(crate::ENV_AUTH).unwrap_or_default();
            let empty: [(&str, &str); 0] = [];
            match provider.to_ascii_lowercase().as_str() {
                "aws" => Self::new_aws(config, empty),
                "azure" => Self::new_azure(config, empty),
                "gcp" | "google" => Self::new_google(config, empty),
                "databricks" => Self::new_databricks(config, empty),
                other => Err(Error::InvalidBaseUrl(format!(
                    "unknown {}={other:?} (expected aws|azure|gcp|databricks)",
                    crate::ENV_AUTH
                ))),
            }
        }
    }
}
