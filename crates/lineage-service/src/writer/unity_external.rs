//! `unity-external` sink: append events to a Unity Catalog **external** Delta table.
//!
//! Resolves the table's storage location from UC once at startup (the table must be
//! pre-created), then on each append vends fresh `ReadWrite` credentials and writes via plain
//! delta-rs (direct commit — no commit coordinator). Only compiled with the `unity` feature.

use std::sync::Arc;

use async_trait::async_trait;
use deltalake::DeltaTableBuilder;
use deltalake::arrow::array::RecordBatch;
use deltalake::protocol::SaveMode;
use unitycatalog_object_store::{TableOperation, UnityObjectStoreFactory};
use url::Url;

use crate::config::UnityTarget;
use crate::writer::sink::{SinkError, TableSink};
use crate::writer::unity::{UnitySinkError, build_factory, ensure_trailing_slash};

/// Appends to a UC external Delta table. The table is pre-created; we resolve its
/// `storage_location` once and re-vend write credentials per append (they expire).
pub struct UnityExternalSink {
    factory: Arc<UnityObjectStoreFactory>,
    /// Three-level name `catalog.schema.table`, used to vend per-table credentials.
    fqn: String,
    /// Table storage location (resolved once at startup).
    location: Url,
    partition_cols: Vec<String>,
}

impl UnityExternalSink {
    /// Connect to UC and resolve the external table's location. Fails fast if the table is
    /// absent — external tables must be pre-created/registered in UC.
    pub async fn connect(target: UnityTarget) -> Result<Self, UnitySinkError> {
        let factory = Arc::new(
            build_factory(
                &target.endpoint,
                target.token.clone(),
                target.region.clone(),
            )
            .await?,
        );
        let loaded = factory
            .unity_client()
            .delta_v1()
            .load_table(&target.catalog, &target.schema, &target.table)
            .await
            .map_err(|e| {
                UnitySinkError::other(format!(
                    "unity-external table {}.{}.{} could not be loaded ({e}); \
                     external tables must be pre-created in Unity Catalog",
                    target.catalog, target.schema, target.table
                ))
            })?;
        let location = Url::parse(&ensure_trailing_slash(&loaded.metadata.location))
            .map_err(|e| UnitySinkError::other(format!("invalid table location: {e}")))?;
        let fqn = format!("{}.{}.{}", target.catalog, target.schema, target.table);
        tracing::info!("unity-external sink: writing to {fqn} at {location}");
        Ok(Self {
            factory,
            fqn,
            location,
            partition_cols: target.partition_cols,
        })
    }

    async fn append_inner(&self, batch: RecordBatch) -> Result<(), UnitySinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // Vend fresh ReadWrite credentials (they expire) and write via plain delta-rs with the
        // credentialed store injected (ADR-0009 direct-commit path). `self.location` is already
        // a parsed URL; don't round-trip it through `ensure_table_uri`, which would reject
        // `s3://` because we never register delta-rs's own S3 factory (the store is injected).
        let store = self
            .factory
            .for_table(self.fqn.clone(), TableOperation::ReadWrite)
            .await?;
        let url = self.location.clone();
        let table = DeltaTableBuilder::from_url(url.clone())
            .map_err(|e| UnitySinkError::Delta(e.to_string()))?
            .with_storage_backend(store.root(), url)
            .load()
            .await
            .map_err(|e| UnitySinkError::Delta(e.to_string()))?;

        let mut op = table.write(vec![batch]).with_save_mode(SaveMode::Append);
        if !self.partition_cols.is_empty() {
            op = op.with_partition_columns(self.partition_cols.clone());
        }
        op.await.map_err(|e| UnitySinkError::Delta(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl TableSink for UnityExternalSink {
    fn name(&self) -> &'static str {
        "unity-external"
    }

    async fn append(&self, batch: RecordBatch) -> Result<(), SinkError> {
        self.append_inner(batch)
            .await
            .map_err(|e| SinkError::Unity(e.to_string()))
    }
}
