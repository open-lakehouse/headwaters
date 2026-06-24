//! Column-lineage processor: the `columnLineage` dataset facet → per-edge
//! `column_lineage_edges` rows.
//!
//! Reads the writer-lifted `column_lineage` column on the event — the document
//! `{inputs:[...], outputs:[{namespace,name,columnLineage}]}` produced by
//! `writer::row::event_to_row`. Column lineage is defined on **output**
//! datasets, so we walk `outputs[]`, and for each output field's `inputFields`
//! emit one [`Mutation::UpsertColumnEdge`] (input field → output field).

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::projection::RawEvent;
use crate::projection::mutation::Mutation;
use crate::projection::processor::FacetProcessor;

pub struct ColumnLineageProcessor;

impl FacetProcessor for ColumnLineageProcessor {
    fn name(&self) -> &'static str {
        "columnLineage"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        let Some(doc) = &ev.column_lineage else {
            return;
        };
        let at = ev
            .event_time
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0));

        let Some(outputs) = doc.get("outputs").and_then(|o| o.as_array()) else {
            return;
        };
        for output in outputs {
            let (Some(out_ns), Some(out_ds)) = (
                output.get("namespace").and_then(|v| v.as_str()),
                output.get("name").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            let Some(fields) = output
                .get("columnLineage")
                .and_then(|cl| cl.get("fields"))
                .and_then(|f| f.as_object())
            else {
                continue;
            };
            for (out_field, lineage) in fields {
                for input in lineage
                    .get("inputFields")
                    .and_then(|v| v.as_array())
                    .into_iter()
                    .flatten()
                {
                    let (Some(in_ns), Some(in_ds), Some(in_field)) = (
                        input.get("namespace").and_then(|v| v.as_str()),
                        input.get("name").and_then(|v| v.as_str()),
                        input.get("field").and_then(|v| v.as_str()),
                    ) else {
                        continue;
                    };
                    out.push(Mutation::UpsertColumnEdge {
                        in_namespace: in_ns.to_string(),
                        in_dataset: in_ds.to_string(),
                        in_field: in_field.to_string(),
                        out_namespace: out_ns.to_string(),
                        out_dataset: out_ds.to_string(),
                        out_field: out_field.clone(),
                        transformation: first_transformation(input),
                        at,
                    });
                }
            }
        }
    }
}

/// The input field's `transformations` array, if present (stored as-is).
fn first_transformation(input: &JsonValue) -> Option<JsonValue> {
    input
        .get("transformations")
        .filter(|t| t.as_array().is_some_and(|a| !a.is_empty()))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event_with_column_lineage() -> RawEvent {
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
            raw: None,
            inputs: None,
            outputs: None,
            // The lifted shape writer::row produces.
            column_lineage: Some(json!({
                "outputs": [{
                    "namespace": "warehouse", "name": "silver",
                    "columnLineage": {"fields": {
                        "id": {"inputFields": [
                            {"namespace": "raw", "name": "customers", "field": "id",
                             "transformations": [{"type": "DIRECT", "subtype": "IDENTITY"}]}
                        ]},
                        "email_hash": {"inputFields": [
                            {"namespace": "raw", "name": "customers", "field": "email"}
                        ]}
                    }}
                }]
            })),
        }
    }

    #[test]
    fn emits_one_edge_per_input_field() {
        let mut out = Vec::new();
        ColumnLineageProcessor.process(&event_with_column_lineage(), &mut out);
        assert_eq!(out.len(), 2);
        let edges: Vec<_> = out
            .iter()
            .map(|m| match m {
                Mutation::UpsertColumnEdge {
                    in_field,
                    out_field,
                    transformation,
                    ..
                } => (
                    in_field.as_str(),
                    out_field.as_str(),
                    transformation.is_some(),
                ),
                other => panic!("expected UpsertColumnEdge, got {other:?}"),
            })
            .collect();
        assert!(edges.contains(&("id", "id", true)));
        assert!(edges.contains(&("email", "email_hash", false)));
    }

    #[test]
    fn no_column_lineage_emits_nothing() {
        let mut ev = event_with_column_lineage();
        ev.column_lineage = None;
        let mut out = Vec::new();
        ColumnLineageProcessor.process(&ev, &mut out);
        assert!(out.is_empty());
    }
}
