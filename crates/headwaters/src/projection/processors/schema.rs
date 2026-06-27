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
        let run_id = ev.run_id.clone();

        // Datasets can carry a schema on inputs, outputs, or a standalone
        // `dataset` (DatasetEvent). Process whichever are present.
        for key in ["inputs", "outputs"] {
            if let Some(arr) = raw.get(key).and_then(|v| v.as_array()) {
                for ds in arr {
                    emit_fields_for(ds, at, run_id.as_deref(), out);
                }
            }
        }
        if let Some(ds) = raw.get("dataset") {
            emit_fields_for(ds, at, run_id.as_deref(), out);
        }
    }
}

/// The OpenLineage dataset-version namespace UUID (a fixed, arbitrary v5 root)
/// under which we derive deterministic per-schema version ids.
const DATASET_VERSION_NS: uuid::Uuid = uuid::uuid!("6f9619ff-8b86-d011-b42d-00c04fc964ff");

/// A deterministic version UUID for a dataset's schema snapshot: UUIDv5 over
/// `namespace/name` + the JSON-serialized field list. The same schema always
/// yields the same id, so re-emitting it is a no-op and replay is idempotent.
pub(crate) fn schema_version_uuid(namespace: &str, name: &str, fields: &[JsonValue]) -> uuid::Uuid {
    let mut key = format!("{namespace}/{name}\u{1}");
    key.push_str(&serde_json::to_string(fields).unwrap_or_default());
    uuid::Uuid::new_v5(&DATASET_VERSION_NS, key.as_bytes())
}

/// Emit `UpsertDatasetField` mutations (one per column) plus an
/// `EmitDatasetVersion` snapshot for one dataset object carrying a
/// `facets.schema` facet.
fn emit_fields_for(
    ds: &JsonValue,
    at: DateTime<Utc>,
    run_id: Option<&str>,
    out: &mut Vec<Mutation>,
) {
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

    // Per-column rows.
    let mut field_values = Vec::with_capacity(schema.fields.len());
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
        // The fields snapshot for the version, in the `datasets.fields` cache
        // shape (name/type, matching what the read path serializes).
        let mut fv = serde_json::Map::new();
        fv.insert("name".into(), JsonValue::String(field.name.clone()));
        if !field.r#type.is_empty() {
            fv.insert("type".into(), JsonValue::String(field.r#type.clone()));
        }
        field_values.push(JsonValue::Object(fv));
    }

    if field_values.is_empty() {
        return;
    }

    // A version snapshot keyed to the producing run, deduped by a deterministic
    // schema hash (the applier inserts ON CONFLICT DO NOTHING).
    out.push(Mutation::EmitDatasetVersion {
        namespace: namespace.to_string(),
        name: name.to_string(),
        version: schema_version_uuid(namespace, name, &field_values),
        run_id: run_id.map(str::to_string),
        fields: field_values,
        at,
    });
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
        // Two field rows + one dataset-version snapshot.
        let fields: Vec<&Mutation> = out
            .iter()
            .filter(|m| matches!(m, Mutation::UpsertDatasetField { .. }))
            .collect();
        assert_eq!(fields.len(), 2);
        assert_eq!(
            out.iter()
                .filter(|m| matches!(m, Mutation::EmitDatasetVersion { .. }))
                .count(),
            1,
            "one version snapshot emitted"
        );
        match fields[0] {
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
        match fields[1] {
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
    fn version_uuid_is_deterministic_per_schema() {
        let a = json!([{"name": "id", "type": "BIGINT"}]);
        let b = json!([{"name": "id", "type": "STRING"}]); // type changed
        let va = schema_version_uuid("ns", "d", a.as_array().unwrap());
        let va2 = schema_version_uuid("ns", "d", a.as_array().unwrap());
        let vb = schema_version_uuid("ns", "d", b.as_array().unwrap());
        assert_eq!(va, va2, "same schema -> same version");
        assert_ne!(va, vb, "different schema -> different version");
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
