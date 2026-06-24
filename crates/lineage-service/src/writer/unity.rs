//! Shared helpers for the Unity Catalog Delta sinks (`unity-external` + `unity-managed`).
//!
//! Only compiled with the `unity` cargo feature.

use datafusion_unitycatalog::managed::CreateManagedTableError;
use unitycatalog_object_store::UnityObjectStoreFactory;

/// Ensure a table location URL ends with a trailing slash so it joins cleanly as a
/// prefix. Re-exported from unitycatalog-rs (the single canonical implementation) so
/// the existing `crate::writer::unity::ensure_trailing_slash` call sites are unchanged.
pub(crate) use datafusion_unitycatalog::catalog::ensure_trailing_slash;

/// Engine identifier recorded in commits written by this service.
pub(crate) const ENGINE_INFO: &str = concat!("lineage-service/", env!("CARGO_PKG_VERSION"));

/// Errors from the Unity Catalog sinks. Mapped to [`SinkError::Unity`](super::sink::SinkError)
/// at the `TableSink` boundary.
#[derive(Debug, thiserror::Error)]
pub enum UnitySinkError {
    #[error("unity catalog client: {0}")]
    Client(#[from] unitycatalog_client::Error),
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("managed table: {0}")]
    Managed(#[from] CreateManagedTableError),
    #[error("delta: {0}")]
    Delta(String),
    #[error("{0}")]
    Other(String),
}

impl UnitySinkError {
    pub(crate) fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

/// Build a [`UnityObjectStoreFactory`] from an endpoint, optional bearer token, and optional
/// AWS region. With no token we allow an unauthenticated server (local OSS).
pub(crate) async fn build_factory(
    endpoint: &str,
    token: Option<String>,
    region: Option<String>,
) -> Result<UnityObjectStoreFactory, UnitySinkError> {
    let mut builder = UnityObjectStoreFactory::builder().with_uri(endpoint);
    match token {
        Some(t) => builder = builder.with_token(Some(t)),
        None => builder = builder.with_allow_unauthenticated(true),
    }
    if let Some(r) = region {
        builder = builder.with_aws_region(Some(r));
    }
    builder.build().await.map_err(UnitySinkError::from)
}

/// Whether a UC client error is a "table not found" (HTTP 404) — used to decide whether to
/// auto-create a managed table. The live Java server returns the delta-API 404 as an untyped
/// `Other { status: 404 }` (its body uses `NoSuchTableException`, not the
/// `RESOURCE_NOT_FOUND` code the typed `NotFound` variant keys on), so match on the HTTP
/// status, not just the typed variant.
pub(crate) fn is_table_not_found(err: &unitycatalog_client::Error) -> bool {
    err.is_not_found()
        || matches!(err, unitycatalog_client::Error::Api(api) if api.http_status() == 404)
}
