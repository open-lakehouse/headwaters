//! Read layer: a Marquez-compatible REST API served over the Delta events
//! table the ingest path writes to.
//!
//! The ingest side ([`crate::writer`]) only ever appends raw OpenLineage events.
//! Marquez's web UI, by contrast, expects a *materialized* model — namespaces,
//! jobs, datasets, and a lineage graph with edges. This module reconstructs that
//! model on read by querying the events table with DataFusion ([`queries`]) and
//! shaping the result into Marquez's JSON contract ([`model`]). [`http`] mounts
//! the endpoints the UI needs under `/api/v1`.
//!
//! The store re-opens the Delta table on every query so freshly ingested events
//! are visible without a restart. Query volume for a lineage UI is low, so the
//! per-request `open_table` cost is acceptable. The queries push their column
//! projection (and, for the events/run-facets endpoints, row filters and
//! pagination) down into DataFusion so we don't materialize the whole log per
//! request. The model-folding endpoints still scan every event row each call;
//! that grows unbounded with the log. FOLLOW-UP: maintain a materialized model
//! (incremental fold on ingest, or a periodic snapshot) and/or partition/index
//! the events table so reads don't re-fold history. Deferred — out of scope for
//! the read-correctness pass.

pub mod http;
pub mod model;
pub mod queries;

use std::collections::HashMap;

use deltalake::datafusion::prelude::SessionContext;
use deltalake::{DeltaTableError, ensure_table_uri, open_table_with_storage_options};

#[cfg(feature = "unity")]
use std::sync::Arc;

#[cfg(feature = "unity")]
use datafusion_unitycatalog::catalog::{
    ManagedReadState, build_catalog_managed_snapshot, resolve_managed_read_state,
};
#[cfg(feature = "unity")]
use deltalake::datafusion::datasource::TableProvider;
#[cfg(feature = "unity")]
use deltalake::delta_datafusion::DeltaScanNext;
#[cfg(feature = "unity")]
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
#[cfg(feature = "unity")]
use deltalake::delta_datafusion::engine::DataFusionEngine;
#[cfg(feature = "unity")]
use deltalake::logstore::{StorageConfig, default_logstore};
#[cfg(feature = "unity")]
use deltalake::{DeltaTable, DeltaTableConfig};
#[cfg(feature = "unity")]
use object_store::path::Path as ObjectStorePath;
#[cfg(feature = "unity")]
use object_store::prefix::PrefixStore;
#[cfg(feature = "unity")]
use unitycatalog_object_store::{TableOperation, UnityObjectStoreFactory};

use crate::config::Config;
#[cfg(feature = "unity")]
use crate::writer::unity::{build_factory, ensure_trailing_slash, is_table_not_found};

/// Column names of the events table, kept in one place so the read queries don't
/// drift from [`crate::writer::schema::arrow_schema`]. Referenced in
/// [`queries`].
pub mod columns {
    pub const EVENT_KIND: &str = "event_kind";
    pub const EVENT_TYPE: &str = "event_type";
    pub const EVENT_TIME: &str = "event_time";
    pub const RUN_ID: &str = "run_id";
    pub const JOB_NAMESPACE: &str = "job_namespace";
    pub const JOB_NAME: &str = "job_name";
    pub const DATASET_NAMESPACE: &str = "dataset_namespace";
    pub const DATASET_NAME: &str = "dataset_name";
    pub const INPUTS_JSON: &str = "inputs_json";
    pub const OUTPUTS_JSON: &str = "outputs_json";
    pub const COLUMN_LINEAGE_JSON: &str = "column_lineage_json";
    pub const RAW_JSON: &str = "raw_json";
}

/// Table name the events table is registered under inside the per-query
/// DataFusion session.
const EVENTS_TABLE: &str = "events";

/// Errors surfaced by the read layer. The HTTP layer maps these onto status
/// codes (404 for not-found, 500 otherwise).
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("failed to initialize the read store: {0}")]
    Init(String),

    #[error("failed to open delta table: {0}")]
    OpenTable(String),

    #[error("query failed: {0}")]
    Query(String),

    #[error("not found: {0}")]
    NotFound(String),
}

impl From<DeltaTableError> for ReadError {
    fn from(e: DeltaTableError) -> Self {
        ReadError::OpenTable(e.to_string())
    }
}

impl From<deltalake::datafusion::error::DataFusionError> for ReadError {
    fn from(e: deltalake::datafusion::error::DataFusionError) -> Self {
        ReadError::Query(e.to_string())
    }
}

/// Read-only handle over the lineage events Delta table.
///
/// Mirrors the writer's table-location modes: `local` reads `delta.table_path`
/// directly; the Unity modes resolve the table's storage location through the
/// catalog and vend per-query `Read` credentials, exactly like the sinks do on
/// the write side. Opens a fresh [`SessionContext`] with the table registered
/// for each query. Cheap to clone — a URI + options map, or an `Arc`'d factory.
#[derive(Clone)]
pub struct LineageStore(StoreTarget);

#[derive(Clone)]
enum StoreTarget {
    /// A Delta table at a fixed local / object-store URI.
    Local {
        table_uri: String,
        storage_options: HashMap<String, String>,
    },
    /// A Unity Catalog table (external or managed): resolve the location and
    /// vend `Read` credentials through the catalog on every session open.
    ///
    /// Managed tables are read off the *published* `_delta_log` (plain
    /// delta-rs). The managed writer publishes + backfills after every commit,
    /// so at worst the UI lags by an unpublished tail until the next write —
    /// acceptable for a lineage browse UI, and far better than the alternative
    /// of rendering nothing.
    #[cfg(feature = "unity")]
    Unity {
        factory: Arc<UnityObjectStoreFactory>,
        catalog: String,
        schema: String,
        table: String,
    },
}

impl LineageStore {
    /// Build a store from the service config, mirroring the writer's resolved
    /// [`DeltaTarget`] so reads and writes target the same table in every mode.
    ///
    /// For the Unity modes this connects to the catalog (same endpoint / token /
    /// region resolution as the sinks), so a misconfigured deployment fails at
    /// startup rather than on the first query.
    pub async fn from_config(cfg: &Config) -> Result<Self, ReadError> {
        let target = cfg
            .delta
            .resolve(&cfg.storage_options)
            .map_err(|e| ReadError::Init(e.to_string()))?;
        match target {
            crate::config::DeltaTarget::Local {
                table_uri,
                storage_options,
                ..
            } => Ok(Self(StoreTarget::Local {
                table_uri,
                storage_options,
            })),
            #[cfg(feature = "unity")]
            crate::config::DeltaTarget::UnityExternal(t)
            | crate::config::DeltaTarget::UnityManaged(t) => {
                let factory = build_factory(&t.endpoint, t.token.clone(), t.region.clone())
                    .await
                    .map_err(|e| ReadError::Init(e.to_string()))?;
                tracing::info!(
                    "lineage read API resolving {}.{}.{} through Unity Catalog",
                    t.catalog,
                    t.schema,
                    t.table
                );
                Ok(Self(StoreTarget::Unity {
                    factory: Arc::new(factory),
                    catalog: t.catalog,
                    schema: t.schema,
                    table: t.table,
                }))
            }
            // Unity targets are rejected by config validation when the feature is off.
            #[cfg(not(feature = "unity"))]
            crate::config::DeltaTarget::UnityExternal(_)
            | crate::config::DeltaTarget::UnityManaged(_) => unreachable!(
                "unity delta mode selected without the `unity` feature; config validation should have rejected this"
            ),
        }
    }

    /// Open the events table and register it on a fresh session as `events`.
    ///
    /// Returns `Ok(None)` when the table does not exist yet (no events have ever
    /// been ingested) so callers can return empty results rather than an error —
    /// a brand-new deployment should render an empty UI, not a 500.
    pub(crate) async fn session(&self) -> Result<Option<SessionContext>, ReadError> {
        match &self.0 {
            StoreTarget::Local {
                table_uri,
                storage_options,
            } => {
                let url =
                    ensure_table_uri(table_uri).map_err(|e| ReadError::OpenTable(e.to_string()))?;
                let table =
                    match open_table_with_storage_options(url, storage_options.clone()).await {
                        Ok(t) => t,
                        // Table not created yet — treat as empty rather than an error.
                        Err(DeltaTableError::NotATable(_))
                        | Err(DeltaTableError::InvalidTableLocation(_)) => return Ok(None),
                        Err(e) => return Err(ReadError::OpenTable(e.to_string())),
                    };
                let ctx = SessionContext::new();
                table
                    .update_datafusion_session(&ctx.state())
                    .map_err(|e| ReadError::OpenTable(e.to_string()))?;
                let provider = table
                    .table_provider()
                    .await
                    .map_err(|e| ReadError::OpenTable(e.to_string()))?;
                ctx.register_table(EVENTS_TABLE, provider)?;
                Ok(Some(ctx))
            }
            #[cfg(feature = "unity")]
            StoreTarget::Unity {
                factory,
                catalog,
                schema,
                table,
            } => open_unity_session(factory, catalog, schema, table).await,
        }
    }
}

/// Build a session that reads a Unity Catalog table through catalog-vended
/// credentials, registering it as `events`.
///
/// Resolves the table once (`load_table`), vends a `Read`-scoped store, registers
/// it on the session's runtime by object-store URL, then routes by managed-ness:
///
/// - **managed** (catalog-coordinated): the catalog — not `_delta_log/` — is the
///   source of truth for the latest version, so build the kernel snapshot from
///   the ratified commit tail ([`build_catalog_managed_snapshot`]) and scan it
///   with [`DeltaScanNext`]. This sees commits the catalog has ratified even if
///   the writer hasn't backfilled them into the published log yet.
/// - **external**: the published `_delta_log/` is authoritative, so open it with
///   [`DeltaTable::new`] over the same injected logstore.
///
/// `Ok(None)` when the table isn't registered in UC yet (the managed writer
/// auto-creates it on the first flush) or has no readable log yet.
#[cfg(feature = "unity")]
async fn open_unity_session(
    factory: &Arc<UnityObjectStoreFactory>,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Option<SessionContext>, ReadError> {
    let loaded = match factory
        .unity_client()
        .delta_v1()
        .load_table(catalog, schema, table)
        .await
    {
        Ok(l) => l,
        Err(e) if is_table_not_found(&e) => return Ok(None),
        Err(e) => return Err(ReadError::OpenTable(e.to_string())),
    };

    let fqn = format!("{catalog}.{schema}.{table}");
    let store = factory
        .for_table(fqn.clone(), TableOperation::Read)
        .await
        .map_err(|e| ReadError::OpenTable(e.to_string()))?;
    // Parse the UC-resolved location directly: `ensure_table_uri` validates the
    // scheme against delta-rs's registered store factories, but we inject the
    // catalog-vended store ourselves, so `s3://` need not be registered.
    let location = url::Url::parse(&ensure_trailing_slash(&loaded.metadata.location))
        .map_err(|e| ReadError::OpenTable(format!("invalid table location: {e}")))?;

    // Register the vended store on the session runtime so the kernel engine and
    // the logstore both resolve it by its object-store URL (scheme://bucket — the
    // key delta-rs derives from the table location).
    let ctx = SessionContext::new();
    let store_url = location.as_object_store_url();
    ctx.runtime_env()
        .register_object_store(store_url.as_ref(), store.root());

    // A logstore over the injected store (no factory-registry lookup, so the
    // `s3` scheme need not be registered process-wide).
    let table_path = ObjectStorePath::from_url_path(location.path())
        .map_err(|e| ReadError::OpenTable(e.to_string()))?;
    let prefixed = Arc::new(PrefixStore::new(store.root(), table_path));
    let storage_config = StorageConfig::default();
    let log_store = default_logstore(prefixed, store.root(), &location, &storage_config);

    let provider: Arc<dyn TableProvider> = match resolve_managed_read_state(&loaded)
        .map_err(|e| ReadError::OpenTable(e.to_string()))?
    {
        ManagedReadState::Managed { commits, latest } => {
            let engine = DataFusionEngine::new_from_context(ctx.task_ctx());
            let snapshot = build_catalog_managed_snapshot(
                engine.as_ref(),
                &location,
                &commits,
                latest as i64,
                None,
            )
            .map_err(|e| ReadError::OpenTable(e.to_string()))?;
            DeltaScanNext::builder()
                .with_snapshot(Arc::new(snapshot))
                .with_log_store(log_store)
                .await
                .map_err(|e| ReadError::OpenTable(e.to_string()))?
        }
        ManagedReadState::NotManaged => {
            let mut dt = DeltaTable::new(log_store, DeltaTableConfig::default());
            match dt.load().await {
                Ok(()) => {}
                // Registered in UC but no published log yet — render empty.
                Err(e @ DeltaTableError::NotATable(_))
                | Err(e @ DeltaTableError::InvalidTableLocation(_)) => {
                    tracing::debug!(table = %fqn, error = %e, "unity table has no readable log yet; rendering empty");
                    return Ok(None);
                }
                Err(e) => return Err(ReadError::OpenTable(e.to_string())),
            }
            dt.table_provider()
                .await
                .map_err(|e| ReadError::OpenTable(e.to_string()))?
        }
    };

    ctx.register_table(EVENTS_TABLE, provider)?;
    Ok(Some(ctx))
}
