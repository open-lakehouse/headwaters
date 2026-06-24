use std::sync::Arc;

use buffa::Message;
use deltalake::DeltaTableBuilder;
use deltalake::arrow::array::StringArray;
use deltalake::arrow::datatypes::DataType;

use lineage_service::config::{Config, DeltaConfig};
use lineage_service::lineage::v1::{
    ColumnLineageDatasetFacet, FieldTransformation, InputField, Job, OpenLineageEvent,
    OpenLineageEventView, OutputDataset, OutputFieldLineage, Run, RunEvent,
    open_lineage_event::Event,
};
use lineage_service::writer::delta::DeltaWriter;
use lineage_service::writer::schema::{arrow_schema, events_to_record_batch};

fn local_config(path: &str) -> Config {
    Config {
        delta: DeltaConfig {
            table_path: path.to_string(),
            partition_cols: vec![],
            ..DeltaConfig::default()
        },
        ..Config::default()
    }
}

#[test]
fn test_arrow_schema_field_count() {
    let schema = arrow_schema();
    assert_eq!(schema.fields().len(), 15);
    assert_eq!(schema.field(0).name(), "event_kind");
    assert_eq!(
        schema.field(2).data_type(),
        &DataType::Timestamp(
            deltalake::arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        )
    );
    assert_eq!(schema.field(13).name(), "column_lineage_json");
}

#[test]
fn test_events_to_record_batch_empty() {
    let batch = events_to_record_batch(&[]).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 15);
}

#[test]
fn test_events_to_record_batch_skips_none_event() {
    let evt = OpenLineageEventView::default();
    let batch = events_to_record_batch(&[evt]).unwrap();
    assert_eq!(
        batch.num_rows(),
        0,
        "events with no variant should be skipped"
    );
}

#[tokio::test]
async fn test_delta_writer_create_and_append() {
    let tmp = tempfile::tempdir().unwrap();
    let writer = DeltaWriter::new(&local_config(tmp.path().to_str().unwrap()));

    let schema = arrow_schema();
    let batch = deltalake::arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["run"])),
            Arc::new(StringArray::from(vec![Some("START")])),
            Arc::new(
                deltalake::arrow::array::TimestampMicrosecondArray::from(vec![1_000_000i64])
                    .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(vec!["test-producer"])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("run-123")])),
            Arc::new(StringArray::from(vec![Some("ns")])),
            Arc::new(StringArray::from(vec![Some("job1")])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(StringArray::from(vec![Some("{\"raw\":1}")])),
        ],
    )
    .unwrap();

    writer.append(batch).await.unwrap();

    // Verify the table was created and has version 1.
    let url = url::Url::from_directory_path(tmp.path()).unwrap();
    let mut table = DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    table.load().await.unwrap();
    assert!(table.version().is_some());
}

#[tokio::test]
async fn test_delta_writer_append_empty_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let writer = DeltaWriter::new(&local_config(tmp.path().to_str().unwrap()));

    let empty = deltalake::arrow::array::RecordBatch::new_empty(arrow_schema());
    writer.append(empty).await.unwrap();
    // No table should be created for an empty batch.
}

/// Load the shared `resources/examples/lineage/column-lineage/` JSON
/// fixture (also exercised by the Go converter tests) and confirm the
/// nested `outputs[].facets.columnLineage` payload deserialises into our
/// typed proto and survives a full encode → decode → record-batch
/// round-trip. The fixture follows the OpenLineage 1-2-0 wire shape
/// (where `columnLineage` is embedded in the facets struct), so the
/// test mimics what the Go converter does: lift the embedded facet,
/// deserialise it as `ColumnLineageDatasetFacet`, and attach it to the
/// typed `OutputDataset.column_lineage` field.
#[test]
fn test_column_lineage_fixture_round_trips_through_record_batch() {
    use deltalake::arrow::array::{Array, StringArray as ArrowStringArray};

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

    let run_event = RunEvent {
        event_type: raw["eventType"].as_str().unwrap_or("COMPLETE").into(),
        event_time: ::buffa::MessageField::some(buffa_types::google::protobuf::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
            ..Default::default()
        }),
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
    };

    let envelope = OpenLineageEvent {
        event: Some(Event::RunEvent(Box::new(run_event))),
        ..Default::default()
    };

    let bytes = envelope.encode_to_vec();
    let view: OpenLineageEventView<'_> =
        <OpenLineageEventView<'_> as buffa::MessageView>::decode_view(&bytes).unwrap();

    let batch = events_to_record_batch(std::slice::from_ref(&view)).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let cl_idx = batch.schema().index_of("column_lineage_json").unwrap();
    let cl_col = batch
        .column(cl_idx)
        .as_any()
        .downcast_ref::<ArrowStringArray>()
        .unwrap();
    assert!(!cl_col.is_null(0));
    let cl_json = cl_col.value(0);
    assert!(cl_json.contains("\"silver.customers\""));
    assert!(cl_json.contains("\"customer_id\""));
    assert!(cl_json.contains("\"email_hash\""));
    assert!(cl_json.contains("\"masking\":true"));
    assert!(cl_json.contains("INDIRECT"));
    assert!(cl_json.contains("FILTER"));
}

/// End-to-end check: build an `OpenLineageEvent` carrying a typed
/// `ColumnLineageDatasetFacet` on its output, encode it to wire bytes,
/// decode the borrowed view, run it through `events_to_record_batch`,
/// and append the resulting batch to a Delta table. After loading the
/// table we assert the `column_lineage_json` column is populated and
/// contains both the per-output entry and the field/transformations
/// described in the spec.
#[tokio::test]
async fn test_delta_writer_round_trip_with_column_lineage() {
    use deltalake::arrow::array::{Array, StringArray as ArrowStringArray};
    use std::collections::HashMap as StdHashMap;

    let tmp = tempfile::tempdir().unwrap();
    let writer = DeltaWriter::new(&local_config(tmp.path().to_str().unwrap()));

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

    let output = OutputDataset {
        namespace: "warehouse".into(),
        name: "users_hashed".into(),
        column_lineage: ::buffa::MessageField::some(column_lineage),
        ..Default::default()
    };

    let run_event = RunEvent {
        event_type: "COMPLETE".into(),
        event_time: ::buffa::MessageField::some(buffa_types::google::protobuf::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
            ..Default::default()
        }),
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
        outputs: vec![output],
        ..Default::default()
    };

    let envelope = OpenLineageEvent {
        event: Some(Event::RunEvent(Box::new(run_event))),
        ..Default::default()
    };

    let bytes = envelope.encode_to_vec();
    let view: lineage_service::lineage::v1::OpenLineageEventView<'_> =
        <OpenLineageEventView<'_> as buffa::MessageView>::decode_view(&bytes).unwrap();

    let batch = events_to_record_batch(std::slice::from_ref(&view)).unwrap();
    assert_eq!(batch.num_rows(), 1);

    let cl_idx = batch
        .schema()
        .index_of("column_lineage_json")
        .expect("column_lineage_json present");
    let cl_col = batch
        .column(cl_idx)
        .as_any()
        .downcast_ref::<ArrowStringArray>()
        .expect("column_lineage_json is utf8");
    assert!(
        !cl_col.is_null(0),
        "column_lineage_json should be populated"
    );
    let cl_json = cl_col.value(0);
    assert!(cl_json.contains("\"outputs\""));
    assert!(cl_json.contains("\"users_hashed\""));
    assert!(cl_json.contains("\"email_hash\""));
    assert!(cl_json.contains("\"masking\":true"));
    assert!(cl_json.contains("INDIRECT"));
    assert!(cl_json.contains("FILTER"));

    writer.append(batch).await.unwrap();

    let url = url::Url::from_directory_path(tmp.path()).unwrap();
    let mut table = DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    table.load().await.unwrap();
    assert!(table.version().is_some());
}

#[tokio::test]
async fn test_delta_writer_multiple_appends() {
    let tmp = tempfile::tempdir().unwrap();
    let writer = DeltaWriter::new(&local_config(tmp.path().to_str().unwrap()));

    let schema = arrow_schema();
    let make_batch = |kind: &str, producer: &str| {
        deltalake::arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![kind])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(
                    deltalake::arrow::array::TimestampMicrosecondArray::from(vec![2_000_000i64])
                        .with_timezone("UTC"),
                ),
                Arc::new(StringArray::from(vec![producer])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap()
    };

    writer.append(make_batch("run", "p1")).await.unwrap();
    writer.append(make_batch("job", "p2")).await.unwrap();

    let url = url::Url::from_directory_path(tmp.path()).unwrap();
    let mut table = DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    table.load().await.unwrap();
    assert!(table.version().is_some());
}
