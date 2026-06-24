//! Schema processor: the `schema` dataset facet → per-column `dataset_fields`
//! rows.
//!
//! Reads `inputs[]`/`outputs[]`/`dataset.facets.schema` from the raw event,
//! parsing each into the typed buffa [`SchemaDatasetFacet`] (robust to camel/
//! snake casing). Emits one [`Mutation::UpsertDatasetField`] per column. The
//! denormalized `datasets.fields` cache is still maintained by the core
//! [`DatasetRefProcessor`](super::core::DatasetRefProcessor); this adds the
//! first-class per-column rows that column lineage and field tags join against.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::lineage::v1::SchemaDatasetFacet;
use crate::projection::RawEvent;
use crate::projection::mutation::Mutation;
use crate::projection::processor::FacetProcessor;

pub struct SchemaProcessor;

impl FacetProcessor for SchemaProcessor {
    fn name(&self) -> &'static str {
        "schema"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        let Some(raw) = &ev.raw else { return };
        let at = ev
            .event_time
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0));

        // Datasets can carry a schema on inputs, outputs, or a standalone
        // `dataset` (DatasetEvent). Process whichever are present.
        for key in ["inputs", "outputs"] {
            if let Some(arr) = raw.get(key).and_then(|v| v.as_array()) {
                for ds in arr {
                    emit_fields_for(ds, at, out);
                }
            }
        }
        if let Some(ds) = raw.get("dataset") {
            emit_fields_for(ds, at, out);
        }
    }
}

/// Emit `UpsertDatasetField` mutations for one dataset object that carries a
/// `facets.schema` facet.
fn emit_fields_for(ds: &JsonValue, at: DateTime<Utc>, out: &mut Vec<Mutation>) {
    let (Some(namespace), Some(name)) = (
        ds.get("namespace").and_then(|v| v.as_str()),
        ds.get("name").and_then(|v| v.as_str()),
    ) else {
        return;
    };
    let Some(schema_val) = ds.get("facets").and_then(|f| f.get("schema")) else {
        return;
    };
    // Parse via the typed buffa facet — handles `_producer`/`_schemaURL` and
    // both field-name casings.
    let Ok(schema) = serde_json::from_value::<SchemaDatasetFacet>(schema_val.clone()) else {
        return;
    };
    for (ordinal, field) in schema.fields.iter().enumerate() {
        if field.name.is_empty() {
            continue;
        }
        out.push(Mutation::UpsertDatasetField {
            namespace: namespace.to_string(),
            dataset: name.to_string(),
            field: field.name.clone(),
            field_type: (!field.r#type.is_empty()).then(|| field.r#type.clone()),
            description: (!field.description.is_empty()).then(|| field.description.clone()),
            ordinal: ordinal as i32,
            at,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::processor::FacetProcessor;
    use serde_json::json;

    fn event_with_output_schema() -> RawEvent {
        RawEvent {
            seq: 1,
            event_kind: "run".into(),
            event_type: Some("COMPLETE".into()),
            event_time: DateTime::<Utc>::from_timestamp(1_700_000_000, 0),
            run_id: Some("r1".into()),
            job_namespace: Some("etl".into()),
            job_name: Some("j".into()),
            dataset_namespace: None,
            dataset_name: None,
            raw: Some(json!({
                "outputs": [{
                    "namespace": "warehouse", "name": "silver",
                    "facets": {"schema": {"fields": [
                        {"name": "id", "type": "BIGINT"},
                        {"name": "email", "type": "STRING", "description": "addr"}
                    ]}}
                }]
            })),
            inputs: None,
            outputs: None,
            column_lineage: None,
        }
    }

    #[test]
    fn emits_one_field_per_column_with_ordinal() {
        let mut out = Vec::new();
        SchemaProcessor.process(&event_with_output_schema(), &mut out);
        assert_eq!(out.len(), 2);
        match &out[0] {
            Mutation::UpsertDatasetField {
                namespace,
                dataset,
                field,
                field_type,
                ordinal,
                ..
            } => {
                assert_eq!(namespace, "warehouse");
                assert_eq!(dataset, "silver");
                assert_eq!(field, "id");
                assert_eq!(field_type.as_deref(), Some("BIGINT"));
                assert_eq!(*ordinal, 0);
            }
            other => panic!("expected UpsertDatasetField, got {other:?}"),
        }
        match &out[1] {
            Mutation::UpsertDatasetField {
                field,
                description,
                ordinal,
                ..
            } => {
                assert_eq!(field, "email");
                assert_eq!(description.as_deref(), Some("addr"));
                assert_eq!(*ordinal, 1);
            }
            other => panic!("expected UpsertDatasetField, got {other:?}"),
        }
    }

    #[test]
    fn no_schema_emits_nothing() {
        let mut ev = event_with_output_schema();
        ev.raw = Some(json!({"outputs": [{"namespace": "w", "name": "s"}]}));
        let mut out = Vec::new();
        SchemaProcessor.process(&ev, &mut out);
        assert!(out.is_empty());
    }
}
