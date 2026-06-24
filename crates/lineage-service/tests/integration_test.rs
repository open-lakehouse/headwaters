//! Row-extraction round-trip tests (no database).
//!
//! These build an `OpenLineageEvent` carrying a typed
//! `ColumnLineageDatasetFacet`, encode it to wire bytes, decode the borrowed
//! view, and run it through `event_to_row` — the conversion the Postgres sink
//! persists. They assert the promoted columns and the `column_lineage` JSON
//! shape, the part of the old Delta `events_to_record_batch` round-trip that
//! still matters now that storage is row-oriented.

use buffa::Message;

use lineage_service::lineage::v1::{
    ColumnLineageDatasetFacet, FieldTransformation, InputField, Job, OpenLineageEvent,
    OpenLineageEventView, OutputDataset, OutputFieldLineage, Run, RunEvent,
    open_lineage_event::Event,
};
use lineage_service::writer::row::event_to_row;

/// Decode wire bytes into a borrowed view and extract its row.
fn row_of(envelope: &OpenLineageEvent) -> lineage_service::writer::row::EventRow {
    let bytes = envelope.encode_to_vec();
    let view: OpenLineageEventView<'_> =
        <OpenLineageEventView<'_> as buffa::MessageView>::decode_view(&bytes).unwrap();
    event_to_row(&view).expect("event yields a row")
}

#[test]
fn none_event_yields_no_row() {
    let view = OpenLineageEventView::default();
    assert!(event_to_row(&view).is_none(), "empty event is skipped");
}

#[test]
fn run_event_promotes_columns() {
    let envelope = OpenLineageEvent {
        event: Some(Event::RunEvent(Box::new(RunEvent {
            event_type: "START".into(),
            event_time: ::buffa::MessageField::some(buffa_types::google::protobuf::Timestamp {
                seconds: 1_000,
                nanos: 0,
                ..Default::default()
            }),
            run: ::buffa::MessageField::some(Run {
                run_id: "run-123".into(),
                ..Default::default()
            }),
            job: ::buffa::MessageField::some(Job {
                namespace: "ns".into(),
                name: "job1".into(),
                ..Default::default()
            }),
            producer: "test-producer".into(),
            ..Default::default()
        }))),
        ..Default::default()
    };
    let row = row_of(&envelope);
    assert_eq!(row.event_kind, "run");
    assert_eq!(row.event_type.as_deref(), Some("START"));
    assert_eq!(row.run_id.as_deref(), Some("run-123"));
    assert_eq!(row.job_namespace.as_deref(), Some("ns"));
    assert_eq!(row.job_name.as_deref(), Some("job1"));
    assert!(row.event_time.is_some());
    assert!(row.column_lineage.is_none());
}

/// The shared `resources/examples/lineage/column-lineage/` fixture (also
/// exercised by the Go converter tests) round-trips its nested
/// `outputs[].facets.columnLineage` through the typed proto and into the row's
/// `column_lineage` JSON.
#[test]
fn column_lineage_fixture_round_trips_into_row() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/lineage/column-lineage/run-event-with-column-lineage.json",
    );
    let json = std::fs::read_to_string(path).expect("fixture readable");
    let raw: serde_json::Value = serde_json::from_str(&json).expect("fixture is valid JSON");

    let outputs_raw = raw["outputs"].as_array().expect("outputs array present");
    assert_eq!(outputs_raw.len(), 1, "fixture has exactly one output");
    let output_raw = &outputs_raw[0];
    let cl_raw = &output_raw["facets"]["columnLineage"];
    let column_lineage: ColumnLineageDatasetFacet =
        serde_json::from_value(cl_raw.clone()).expect("columnLineage deserialises");

    let output = OutputDataset {
        namespace: output_raw["namespace"].as_str().unwrap().into(),
        name: output_raw["name"].as_str().unwrap().into(),
        column_lineage: ::buffa::MessageField::some(column_lineage),
        ..Default::default()
    };
    let envelope = OpenLineageEvent {
        event: Some(Event::RunEvent(Box::new(RunEvent {
            event_type: raw["eventType"].as_str().unwrap_or("COMPLETE").into(),
            run: ::buffa::MessageField::some(Run {
                run_id: raw["run"]["runId"].as_str().unwrap_or("").into(),
                ..Default::default()
            }),
            job: ::buffa::MessageField::some(Job {
                namespace: raw["job"]["namespace"].as_str().unwrap_or("").into(),
                name: raw["job"]["name"].as_str().unwrap_or("").into(),
                ..Default::default()
            }),
            producer: raw["producer"].as_str().unwrap_or("test-producer").into(),
            outputs: vec![output],
            ..Default::default()
        }))),
        ..Default::default()
    };

    let row = row_of(&envelope);
    let cl = row.column_lineage.expect("column_lineage populated");
    let cl_json = serde_json::to_string(&cl).unwrap();
    assert!(cl_json.contains("\"silver.customers\""));
    assert!(cl_json.contains("\"customer_id\""));
    assert!(cl_json.contains("\"email_hash\""));
    assert!(cl_json.contains("\"masking\":true"));
    assert!(cl_json.contains("INDIRECT"));
    assert!(cl_json.contains("FILTER"));
}

/// A constructed typed facet on an output dataset surfaces in the row's
/// `column_lineage` JSON with the per-output entry, fields, and transformations.
#[test]
fn constructed_column_lineage_surfaces_in_row() {
    use std::collections::HashMap as StdHashMap;

    let mut fields_map: StdHashMap<String, OutputFieldLineage> = StdHashMap::new();
    fields_map.insert(
        "email_hash".into(),
        OutputFieldLineage {
            input_fields: vec![InputField {
                namespace: "warehouse".into(),
                name: "users".into(),
                field: "email".into(),
                transformations: vec![FieldTransformation {
                    r#type: "DIRECT".into(),
                    subtype: "TRANSFORMATION".into(),
                    description: "md5".into(),
                    masking: true,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
    );

    let column_lineage = ColumnLineageDatasetFacet {
        fields: fields_map,
        dataset: vec![InputField {
            namespace: "warehouse".into(),
            name: "users".into(),
            field: "user_id".into(),
            transformations: vec![FieldTransformation {
                r#type: "INDIRECT".into(),
                subtype: "FILTER".into(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let envelope = OpenLineageEvent {
        event: Some(Event::RunEvent(Box::new(RunEvent {
            event_type: "COMPLETE".into(),
            run: ::buffa::MessageField::some(Run {
                run_id: "run-789".into(),
                ..Default::default()
            }),
            job: ::buffa::MessageField::some(Job {
                namespace: "etl".into(),
                name: "hash_emails".into(),
                ..Default::default()
            }),
            producer: "test-producer".into(),
            outputs: vec![OutputDataset {
                namespace: "warehouse".into(),
                name: "users_hashed".into(),
                column_lineage: ::buffa::MessageField::some(column_lineage),
                ..Default::default()
            }],
            ..Default::default()
        }))),
        ..Default::default()
    };

    let row = row_of(&envelope);
    // The output dataset ref is captured.
    let outputs = row.outputs.expect("outputs json");
    assert!(outputs.to_string().contains("users_hashed"));
    let cl_json = serde_json::to_string(&row.column_lineage.expect("column_lineage")).unwrap();
    assert!(cl_json.contains("\"outputs\""));
    assert!(cl_json.contains("\"users_hashed\""));
    assert!(cl_json.contains("\"email_hash\""));
    assert!(cl_json.contains("\"masking\":true"));
    assert!(cl_json.contains("INDIRECT"));
    assert!(cl_json.contains("FILTER"));
}
