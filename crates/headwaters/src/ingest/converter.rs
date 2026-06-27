//! OpenLineage wire-JSON → proto event conversion.
//!
//! OpenLineage events on the wire are flat JSON objects discriminated by which
//! top-level fields are present (`run`+`job` → run event, `dataset` → dataset
//! event, `job` only → job event) — there is no oneof wrapper. The proto model
//! ([`OpenLineageEvent`]) wraps the concrete event in a oneof, and lifts the
//! OpenLineage `columnLineage` facet out of the arbitrary `facets` map into a
//! typed `column_lineage` field.
//!
//! buffa's generated serde already speaks proto3-JSON (camelCase field names,
//! RFC3339 timestamps, `Struct` facets), so we deserialize the concrete event
//! body straight into the proto struct and only hand-handle the three things
//! that proto3-JSON deserialization can't: event classification, the
//! `columnLineage` lift, and preserving the original document in `raw_json`.
//!
//! Mirrors the Go converter at
//! `services/lineage/internal/ingest/converter.go`.

use buffa::OwnedView;
use serde_json::Value;

use crate::lineage::v1::open_lineage_event::Event;
use crate::lineage::v1::{
    ColumnLineageDatasetFacet, DatasetEvent, InputDataset, JobEvent, OpenLineageEvent,
    OpenLineageEventView, OutputDataset, RunEvent, StaticDataset,
};

/// An owned, `'static`, `Send` event view ready to be queued for the writer.
pub type OwnedEvent = OwnedView<OpenLineageEventView<'static>>;

#[derive(Debug, thiserror::Error)]
pub enum ConvertError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unable to classify event: must contain run+job, job, or dataset fields")]
    Unclassifiable,
    #[error("eventTime is required")]
    MissingEventTime,
    #[error("failed to encode event: {0}")]
    Encode(String),
}

/// Convert a single OpenLineage JSON document into an owned proto event view.
pub fn convert_event(data: &[u8]) -> Result<OwnedEvent, ConvertError> {
    let raw: Value = serde_json::from_slice(data)?;
    let event = build_event(&raw, data)?;
    OwnedView::from_owned(&event).map_err(|e| ConvertError::Encode(e.to_string()))
}

/// Outcome of converting one element of a batch.
pub struct BatchOutcome {
    pub events: Vec<OwnedEvent>,
    pub failures: Vec<BatchFailure>,
    pub received: usize,
}

pub struct BatchFailure {
    pub index: usize,
    pub reason: String,
}

/// Convert a JSON array of OpenLineage events, collecting per-element failures
/// rather than aborting on the first bad event (mirrors Go's
/// `convertBatchWithErrors`). Returns `Err` only when the body is not a JSON
/// array at all.
pub fn convert_batch(data: &[u8]) -> Result<BatchOutcome, ConvertError> {
    let items: Vec<&serde_json::value::RawValue> = serde_json::from_slice(data)?;
    let mut events = Vec::with_capacity(items.len());
    let mut failures = Vec::new();
    for (i, item) in items.iter().enumerate() {
        match convert_event(item.get().as_bytes()) {
            Ok(ev) => events.push(ev),
            Err(e) => failures.push(BatchFailure {
                index: i,
                reason: e.to_string(),
            }),
        }
    }
    Ok(BatchOutcome {
        received: items.len(),
        events,
        failures,
    })
}

/// Classify `raw` by field presence and deserialize into the matching proto
/// event, then lift column lineage and stamp `raw_json`.
fn build_event(raw: &Value, original: &[u8]) -> Result<OpenLineageEvent, ConvertError> {
    let obj = raw.as_object().ok_or(ConvertError::Unclassifiable)?;
    let has_run = obj.contains_key("run");
    let has_job = obj.contains_key("job");
    let has_dataset = obj.contains_key("dataset");

    // eventTime is required on every OpenLineage event; reject early with a
    // clear message rather than silently producing a null timestamp.
    if obj
        .get("eventTime")
        .and_then(Value::as_str)
        .unwrap_or("")
        .is_empty()
    {
        return Err(ConvertError::MissingEventTime);
    }

    let raw_json = String::from_utf8_lossy(original).into_owned();

    let event = if has_run && has_job {
        let mut re: RunEvent = serde_json::from_value(raw.clone())?;
        lift_inputs(&mut re.inputs, obj.get("inputs"));
        lift_outputs(&mut re.outputs, obj.get("outputs"));
        re.raw_json = raw_json;
        Event::from(re)
    } else if has_dataset && !has_run {
        let mut de: DatasetEvent = serde_json::from_value(raw.clone())?;
        lift_static_dataset(&mut de.dataset, obj.get("dataset"));
        de.raw_json = raw_json;
        Event::from(de)
    } else if has_job && !has_run {
        let mut je: JobEvent = serde_json::from_value(raw.clone())?;
        lift_inputs(&mut je.inputs, obj.get("inputs"));
        lift_outputs(&mut je.outputs, obj.get("outputs"));
        je.raw_json = raw_json;
        Event::from(je)
    } else {
        return Err(ConvertError::Unclassifiable);
    };

    Ok(OpenLineageEvent {
        event: Some(event),
        ..Default::default()
    })
}

/// Lift the OpenLineage `columnLineage` facet out of `facets` into the typed
/// `column_lineage` field for each input dataset. buffa's serde leaves it in
/// the `facets` `Struct`; downstream readers want the typed sibling.
fn lift_inputs(inputs: &mut [InputDataset], raw_inputs: Option<&Value>) {
    let arr = match raw_inputs.and_then(Value::as_array) {
        Some(a) => a,
        None => return,
    };
    for (ds, raw) in inputs.iter_mut().zip(arr.iter()) {
        if let Some(cl) = column_lineage_from_facets(raw) {
            ds.column_lineage = Some(cl).into();
        }
    }
}

fn lift_outputs(outputs: &mut [OutputDataset], raw_outputs: Option<&Value>) {
    let arr = match raw_outputs.and_then(Value::as_array) {
        Some(a) => a,
        None => return,
    };
    for (ds, raw) in outputs.iter_mut().zip(arr.iter()) {
        if let Some(cl) = column_lineage_from_facets(raw) {
            ds.column_lineage = Some(cl).into();
        }
    }
}

fn lift_static_dataset(dataset: &mut buffa::MessageField<StaticDataset>, raw: Option<&Value>) {
    let raw = match raw {
        Some(r) => r,
        None => return,
    };
    if let Some(cl) = column_lineage_from_facets(raw)
        && let Some(ds) = dataset.as_option_mut()
    {
        ds.column_lineage = Some(cl).into();
    }
}

/// Extract and typed-parse `facets.columnLineage` from a raw dataset object.
/// Returns `None` when absent, malformed, or empty — matching Go's
/// `parseColumnLineageFacet`.
fn column_lineage_from_facets(raw_dataset: &Value) -> Option<ColumnLineageDatasetFacet> {
    let cl = raw_dataset
        .as_object()?
        .get("facets")?
        .as_object()?
        .get("columnLineage")?;
    let facet: ColumnLineageDatasetFacet = serde_json::from_value(cl.clone()).ok()?;
    if facet.fields.is_empty() && facet.dataset.is_empty() {
        return None;
    }
    Some(facet)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lineage::v1::open_lineage_event::EventView;

    fn run_json() -> &'static [u8] {
        br#"{"eventType":"COMPLETE","eventTime":"2026-04-28T19:30:00.000Z",
            "producer":"p","run":{"runId":"r1"},
            "job":{"namespace":"ns","name":"j"},
            "inputs":[{"namespace":"in-ns","name":"in"}],
            "outputs":[{"namespace":"out-ns","name":"out"}]}"#
    }

    fn job_json() -> &'static [u8] {
        br#"{"eventTime":"2026-04-28T19:30:00.000Z","producer":"p",
            "job":{"namespace":"ns","name":"j"}}"#
    }

    fn dataset_json() -> &'static [u8] {
        br#"{"eventTime":"2026-04-28T19:30:00.000Z","producer":"p",
            "dataset":{"namespace":"ds-ns","name":"ds"}}"#
    }

    #[test]
    fn classifies_run_event() {
        let ev = convert_event(run_json()).unwrap();
        assert!(matches!(ev.reborrow().event, Some(EventView::RunEvent(_))));
    }

    #[test]
    fn classifies_job_event() {
        let ev = convert_event(job_json()).unwrap();
        assert!(matches!(ev.reborrow().event, Some(EventView::JobEvent(_))));
    }

    #[test]
    fn classifies_dataset_event() {
        let ev = convert_event(dataset_json()).unwrap();
        assert!(matches!(
            ev.reborrow().event,
            Some(EventView::DatasetEvent(_))
        ));
    }

    #[test]
    fn run_takes_precedence_over_dataset_and_job() {
        // run + job + dataset all present -> RunEvent (run+job wins).
        let data = br#"{"eventTime":"2026-04-28T19:30:00.000Z","producer":"p",
            "run":{"runId":"r"},"job":{"namespace":"n","name":"j"},
            "dataset":{"namespace":"d","name":"d"}}"#;
        let ev = convert_event(data).unwrap();
        assert!(matches!(ev.reborrow().event, Some(EventView::RunEvent(_))));
    }

    #[test]
    fn missing_event_time_is_rejected() {
        let data = br#"{"producer":"p","job":{"namespace":"n","name":"j"}}"#;
        assert!(matches!(
            convert_event(data),
            Err(ConvertError::MissingEventTime)
        ));
    }

    #[test]
    fn unclassifiable_is_rejected() {
        let data = br#"{"eventTime":"2026-04-28T19:30:00.000Z","producer":"p"}"#;
        assert!(matches!(
            convert_event(data),
            Err(ConvertError::Unclassifiable)
        ));
    }

    #[test]
    fn preserves_raw_json() {
        let ev = convert_event(run_json()).unwrap();
        match &ev.reborrow().event {
            Some(EventView::RunEvent(re)) => {
                assert!(re.raw_json.contains("\"runId\":\"r1\""));
            }
            _ => panic!("expected run event"),
        }
    }

    #[test]
    fn lifts_column_lineage_from_output_facets() {
        let data = include_bytes!(
            "../../examples/lineage/column-lineage/run-event-with-column-lineage.json"
        );
        let ev = convert_event(data).unwrap();
        let re = match &ev.reborrow().event {
            Some(EventView::RunEvent(re)) => re,
            _ => panic!("expected run event"),
        };
        let out = re.outputs.iter().next().expect("one output");
        let cl = out
            .column_lineage
            .as_option()
            .expect("column lineage lifted onto output dataset");
        // Two output fields (customer_id, email_hash) + one dataset-level dep.
        assert_eq!(cl.fields.len(), 2);
        assert_eq!(cl.dataset.len(), 1);
    }

    #[test]
    fn batch_collects_per_element_failures() {
        let good = std::str::from_utf8(run_json()).unwrap();
        let data = format!("[{good}, {{\"producer\":\"p\"}}]");
        let outcome = convert_batch(data.as_bytes()).unwrap();
        assert_eq!(outcome.received, 2);
        assert_eq!(outcome.events.len(), 1);
        assert_eq!(outcome.failures.len(), 1);
        assert_eq!(outcome.failures[0].index, 1);
    }
}
