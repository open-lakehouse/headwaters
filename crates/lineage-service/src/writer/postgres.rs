//! Postgres event sink: bulk-appends [`EventRow`]s to the `events` table.
//!
//! This is the only write path. Ingest buffers events and flushes them here as
//! a batch; the rows land append-only in `events` (the source of truth), and
//! the async projection worker folds them into the read tables out of band.

use async_trait::async_trait;
use sqlx::{PgPool, QueryBuilder};

use crate::writer::row::EventRow;
use crate::writer::sink::{EventSink, SinkError};

/// Appends events to the `events` table over a shared connection pool.
pub struct PostgresSink {
    pool: PgPool,
}

impl PostgresSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventSink for PostgresSink {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn append(&self, rows: &[EventRow]) -> Result<(), SinkError> {
        if rows.is_empty() {
            return Ok(());
        }
        // Multi-row INSERT in one round-trip. `seq` is BIGSERIAL — assigned by
        // the DB in insertion order, which is the projection cursor.
        let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new(
            "INSERT INTO events (\
                event_kind, event_type, event_time, producer, schema_url, \
                run_id, job_namespace, job_name, dataset_namespace, dataset_name, \
                raw, inputs, outputs, column_lineage) ",
        );
        qb.push_values(rows, |mut b, row| {
            b.push_bind(row.event_kind)
                .push_bind(&row.event_type)
                .push_bind(row.event_time)
                .push_bind(&row.producer)
                .push_bind(&row.schema_url)
                .push_bind(&row.run_id)
                .push_bind(&row.job_namespace)
                .push_bind(&row.job_name)
                .push_bind(&row.dataset_namespace)
                .push_bind(&row.dataset_name)
                .push_bind(&row.raw)
                .push_bind(&row.inputs)
                .push_bind(&row.outputs)
                .push_bind(&row.column_lineage);
        });
        qb.build()
            .execute(&self.pool)
            .await
            .map_err(|e| SinkError::Postgres(e.to_string()))?;
        Ok(())
    }
}
