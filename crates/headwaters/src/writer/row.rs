//! Convert an OpenLineage event view into an [`EventRow`] — the column set the
//! `events` table stores.
//!
//! This is the row-oriented successor to the old Arrow `events_to_record_batch`
//! path: the same field extraction and the same `columnLineage`-lift /
//! input-output JSON shaping, but producing owned values the Postgres sink
//! binds directly. The lineage-relevant scalars are promoted to columns; the
//! original document is preserved in `raw`, and facets ride along inside it.

use chrono::{DateTime, TimeZone, Utc};
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::lineage::v1::open_lineage_event::EventView;
use crate::lineage::v1::{
    ColumnLineageDatasetFacetView, DatasetEventView, FieldTransformationView, InputDatasetView,
    InputFieldView, JobEventView, OpenLineageEventView, OutputDatasetView, OutputFieldLineageView,
    RunEventView,
};

/// One row of the `events` table. `None` fields map to SQL NULL.
#[derive(Debug, Clone)]
pub struct EventRow {
    /// "run" | "job" | "dataset".
    pub event_kind: &'static str,
    pub event_type: Option<String>,
    pub event_time: Option<DateTime<Utc>>,
    pub producer: Option<String>,
    pub schema_url: Option<String>,
    pub run_id: Option<String>,
    pub job_namespace: Option<String>,
    pub job_name: Option<String>,
    pub dataset_namespace: Option<String>,
    pub dataset_name: Option<String>,
    /// The original OpenLineage document.
    pub raw: Option<JsonValue>,
    /// Input dataset references (`[{namespace,name}]`).
    pub inputs: Option<JsonValue>,
    /// Output dataset references (`[{namespace,name}]`).
    pub outputs: Option<JsonValue>,
    /// Per-event typed ColumnLineageDatasetFacet payload, if any dataset carries one.
    pub column_lineage: Option<JsonValue>,
}

/// Convert an event view into its [`EventRow`]. Returns `None` for an empty
/// (`event = None`) view, which the writer skips.
pub fn event_to_row(evt: &OpenLineageEventView<'_>) -> Option<EventRow> {
    match evt.event.as_ref()? {
        EventView::RunEvent(re) => Some(run_row(re)),
        EventView::JobEvent(je) => Some(job_row(je)),
        EventView::DatasetEvent(de) => Some(dataset_row(de)),
    }
}

fn ts_to_utc(ts: &buffa_types::google::protobuf::TimestampView<'_>) -> DateTime<Utc> {
    // `nanos` is an `i32` and a well-formed proto Timestamp keeps it in
    // `[0, 1_000_000_000)`, but a malformed producer can send a negative or
    // out-of-range value. Casting straight to `u32` would wrap a negative into a
    // huge value, `timestamp_opt` would reject it, and the fallback would
    // silently rewrite the event to the epoch — corrupting time ordering. Carry
    // any nanos overflow/underflow into the seconds instead so the instant is
    // preserved.
    let extra_secs = ts.nanos.div_euclid(1_000_000_000) as i64;
    let nanos = ts.nanos.rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(ts.seconds.saturating_add(extra_secs), nanos)
        .single()
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0))
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn parse_raw(raw: &str) -> Option<JsonValue> {
    if raw.is_empty() {
        return None;
    }
    serde_json::from_str(raw).ok()
}

fn run_row(re: &RunEventView<'_>) -> EventRow {
    EventRow {
        event_kind: "run",
        event_type: non_empty(re.event_type),
        event_time: re.event_time.as_option().map(ts_to_utc),
        producer: non_empty(re.producer),
        schema_url: non_empty(re.schema_url),
        run_id: re.run.as_option().and_then(|r| non_empty(r.run_id)),
        job_namespace: re.job.as_option().and_then(|j| non_empty(j.namespace)),
        job_name: re.job.as_option().and_then(|j| non_empty(j.name)),
        dataset_namespace: None,
        dataset_name: None,
        raw: parse_raw(re.raw_json),
        inputs: input_datasets_to_json(&re.inputs),
        outputs: output_datasets_to_json(&re.outputs),
        column_lineage: io_column_lineage(&re.inputs, &re.outputs),
    }
}

fn job_row(je: &JobEventView<'_>) -> EventRow {
    EventRow {
        event_kind: "job",
        event_type: None,
        event_time: je.event_time.as_option().map(ts_to_utc),
        producer: non_empty(je.producer),
        schema_url: non_empty(je.schema_url),
        run_id: None,
        job_namespace: je.job.as_option().and_then(|j| non_empty(j.namespace)),
        job_name: je.job.as_option().and_then(|j| non_empty(j.name)),
        dataset_namespace: None,
        dataset_name: None,
        raw: parse_raw(je.raw_json),
        inputs: input_datasets_to_json(&je.inputs),
        outputs: output_datasets_to_json(&je.outputs),
        column_lineage: io_column_lineage(&je.inputs, &je.outputs),
    }
}

fn dataset_row(de: &DatasetEventView<'_>) -> EventRow {
    let (ns, name) = match de.dataset.as_option() {
        Some(d) => (non_empty(d.namespace), non_empty(d.name)),
        None => (None, None),
    };
    EventRow {
        event_kind: "dataset",
        event_type: None,
        event_time: de.event_time.as_option().map(ts_to_utc),
        producer: non_empty(de.producer),
        schema_url: non_empty(de.schema_url),
        run_id: None,
        job_namespace: None,
        job_name: None,
        dataset_namespace: ns,
        dataset_name: name,
        raw: parse_raw(de.raw_json),
        inputs: None,
        outputs: None,
        column_lineage: dataset_column_lineage(de),
    }
}

fn input_datasets_to_json(datasets: &[InputDatasetView<'_>]) -> Option<JsonValue> {
    refs_to_json(datasets.iter().map(|d| (d.namespace, d.name)))
}

fn output_datasets_to_json(datasets: &[OutputDatasetView<'_>]) -> Option<JsonValue> {
    refs_to_json(datasets.iter().map(|d| (d.namespace, d.name)))
}

fn refs_to_json<'a>(refs: impl Iterator<Item = (&'a str, &'a str)>) -> Option<JsonValue> {
    let arr: Vec<JsonValue> = refs
        .map(|(ns, name)| {
            let mut m = JsonMap::new();
            m.insert("namespace".into(), JsonValue::String(ns.to_string()));
            m.insert("name".into(), JsonValue::String(name.to_string()));
            JsonValue::Object(m)
        })
        .collect();
    if arr.is_empty() {
        None
    } else {
        Some(JsonValue::Array(arr))
    }
}

// --- column lineage shaping (mirrors the OpenLineage 1-2-0 facet JSON) ---

fn field_transformation_to_json(t: &FieldTransformationView<'_>) -> JsonValue {
    let mut m = JsonMap::new();
    if !t.r#type.is_empty() {
        m.insert("type".into(), JsonValue::String(t.r#type.into()));
    }
    if !t.subtype.is_empty() {
        m.insert("subtype".into(), JsonValue::String(t.subtype.into()));
    }
    if !t.description.is_empty() {
        m.insert(
            "description".into(),
            JsonValue::String(t.description.into()),
        );
    }
    if t.masking {
        m.insert("masking".into(), JsonValue::Bool(true));
    }
    JsonValue::Object(m)
}

fn input_field_to_json(f: &InputFieldView<'_>) -> JsonValue {
    let mut m = JsonMap::new();
    m.insert("namespace".into(), JsonValue::String(f.namespace.into()));
    m.insert("name".into(), JsonValue::String(f.name.into()));
    m.insert("field".into(), JsonValue::String(f.field.into()));
    if !f.transformations.is_empty() {
        let arr: Vec<JsonValue> = f
            .transformations
            .iter()
            .map(field_transformation_to_json)
            .collect();
        m.insert("transformations".into(), JsonValue::Array(arr));
    }
    JsonValue::Object(m)
}

fn output_field_lineage_to_json(o: &OutputFieldLineageView<'_>) -> JsonValue {
    let mut m = JsonMap::new();
    let inputs: Vec<JsonValue> = o.input_fields.iter().map(input_field_to_json).collect();
    m.insert("inputFields".into(), JsonValue::Array(inputs));
    if !o.transformation_description.is_empty() {
        m.insert(
            "transformationDescription".into(),
            JsonValue::String(o.transformation_description.into()),
        );
    }
    if !o.transformation_type.is_empty() {
        m.insert(
            "transformationType".into(),
            JsonValue::String(o.transformation_type.into()),
        );
    }
    JsonValue::Object(m)
}

fn column_lineage_to_json(facet: &ColumnLineageDatasetFacetView<'_>) -> Option<JsonValue> {
    if facet.fields.is_empty() && facet.dataset.is_empty() {
        return None;
    }
    let mut m = JsonMap::new();
    if !facet.fields.is_empty() {
        let mut fmap = JsonMap::new();
        for (k, v) in facet.fields.iter() {
            fmap.insert((*k).to_string(), output_field_lineage_to_json(v));
        }
        m.insert("fields".into(), JsonValue::Object(fmap));
    }
    if !facet.dataset.is_empty() {
        let arr: Vec<JsonValue> = facet.dataset.iter().map(input_field_to_json).collect();
        m.insert("dataset".into(), JsonValue::Array(arr));
    }
    Some(JsonValue::Object(m))
}

/// Build the per-event column-lineage document: `{inputs:[...], outputs:[...]}`,
/// each entry `{namespace, name, columnLineage}` for datasets that carry one.
/// Returns `None` when no dataset on the event has column lineage.
fn io_column_lineage(
    inputs: &[InputDatasetView<'_>],
    outputs: &[OutputDatasetView<'_>],
) -> Option<JsonValue> {
    let in_entries: Vec<JsonValue> = inputs
        .iter()
        .filter_map(|d| {
            d.column_lineage
                .as_option()
                .and_then(column_lineage_to_json)
                .map(|cl| ds_cl_entry(d.namespace, d.name, cl))
        })
        .collect();
    let out_entries: Vec<JsonValue> = outputs
        .iter()
        .filter_map(|d| {
            d.column_lineage
                .as_option()
                .and_then(column_lineage_to_json)
                .map(|cl| ds_cl_entry(d.namespace, d.name, cl))
        })
        .collect();
    if in_entries.is_empty() && out_entries.is_empty() {
        return None;
    }
    let mut m = JsonMap::new();
    if !in_entries.is_empty() {
        m.insert("inputs".into(), JsonValue::Array(in_entries));
    }
    if !out_entries.is_empty() {
        m.insert("outputs".into(), JsonValue::Array(out_entries));
    }
    Some(JsonValue::Object(m))
}

fn dataset_column_lineage(de: &DatasetEventView<'_>) -> Option<JsonValue> {
    let ds = de.dataset.as_option()?;
    let cl = ds
        .column_lineage
        .as_option()
        .and_then(column_lineage_to_json)?;
    let mut m = JsonMap::new();
    m.insert("dataset".into(), ds_cl_entry(ds.namespace, ds.name, cl));
    Some(JsonValue::Object(m))
}

fn ds_cl_entry(namespace: &str, name: &str, column_lineage: JsonValue) -> JsonValue {
    let mut m = JsonMap::new();
    m.insert("namespace".into(), JsonValue::String(namespace.to_string()));
    m.insert("name".into(), JsonValue::String(name.to_string()));
    m.insert("columnLineage".into(), column_lineage);
    JsonValue::Object(m)
}
