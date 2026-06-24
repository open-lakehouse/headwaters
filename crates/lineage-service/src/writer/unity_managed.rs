//! `unity-managed` sink: append events to a Unity Catalog **catalog-managed** Delta table via
//! the managed connector (`datafusion_unitycatalog::managed`). Commits are staged + ratified
//! through the catalog. Only compiled with the `unity` cargo feature.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion_unitycatalog::managed::{append_to_managed_table, create_managed_table};
use deltalake::arrow::array::RecordBatch;
use unitycatalog_object_store::UnityObjectStoreFactory;

use crate::config::UnityTarget;
use crate::writer::schema::arrow_schema;
use crate::writer::sink::{SinkError, TableSink};
use crate::writer::unity::{ENGINE_INFO, UnitySinkError, build_factory, is_table_not_found};

/// Appends to a UC catalog-managed table. The managed connector re-resolves the table
/// (`loadTable` + vend creds) on every append, so this sink just holds the factory + name.
pub struct UnityManagedSink {
    factory: Arc<UnityObjectStoreFactory>,
    catalog: String,
    schema: String,
    table: String,
}

impl UnityManagedSink {
    /// Connect to UC, and (if `target.auto_create`) create the managed table when it's absent,
    /// using the 15-column events schema. Resolves/validates at startup so config errors fail
    /// fast before any event is ingested.
    pub async fn connect(target: UnityTarget) -> Result<Self, UnitySinkError> {
        let factory = Arc::new(
            build_factory(
                &target.endpoint,
                target.token.clone(),
                target.region.clone(),
            )
            .await?,
        );
        let client = Arc::new(factory.unity_client().delta_v1());

        match client
            .load_table(&target.catalog, &target.schema, &target.table)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "unity-managed sink: using existing table {}.{}.{}",
                    target.catalog,
                    target.schema,
                    target.table
                );
            }
            Err(e) if target.auto_create && is_table_not_found(&e) => {
                tracing::info!(
                    "unity-managed sink: creating table {}.{}.{}",
                    target.catalog,
                    target.schema,
                    target.table
                );
                create_managed_table(
                    client,
                    &target.catalog,
                    &target.schema,
                    &target.table,
                    arrow_schema(),
                    // Managed appends are unpartitioned in v1; create the table unpartitioned
                    // too so its layout matches what we write.
                    Vec::new(),
                    ENGINE_INFO,
                )
                .await?;
            }
            Err(e) if is_table_not_found(&e) => {
                return Err(UnitySinkError::other(format!(
                    "unity-managed table {}.{}.{} not found and auto_create is disabled; \
                     create it first or set delta.auto_create=true",
                    target.catalog, target.schema, target.table
                )));
            }
            Err(e) => return Err(e.into()),
        }

        Ok(Self {
            factory,
            catalog: target.catalog,
            schema: target.schema,
            table: target.table,
        })
    }

    async fn append_inner(&self, batch: RecordBatch) -> Result<(), UnitySinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        append_to_managed_table(
            self.factory.clone(),
            &self.catalog,
            &self.schema,
            &self.table,
            batch,
            ENGINE_INFO,
        )
        .await?;
        Ok(())
    }
}

#[async_trait]
impl TableSink for UnityManagedSink {
    fn name(&self) -> &'static str {
        "unity-managed"
    }

    async fn append(&self, batch: RecordBatch) -> Result<(), SinkError> {
        self.append_inner(batch)
            .await
            .map_err(|e| SinkError::Unity(e.to_string()))
    }
}
