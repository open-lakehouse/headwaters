//! Run-metadata processor: the `nominalTime`, `parent`, and `errorMessage` run
//! facets → run columns (nominal window, parent link, error message).
//!
//! Reads `raw.run.facets`, parsing each into its typed buffa facet. Emits a
//! single [`Mutation::SetRunMeta`] carrying whichever fields are present;
//! the applier sets only those, latest-wins by event time.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::lineage::v1::{ErrorMessageRunFacet, NominalTimeRunFacet, ParentRunFacet};
use crate::projection::RawEvent;
use crate::projection::mutation::Mutation;
use crate::projection::processor::FacetProcessor;

pub struct RunMetaProcessor;

impl FacetProcessor for RunMetaProcessor {
    fn name(&self) -> &'static str {
        "runMeta"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        let Some(run_id) = &ev.run_id else { return };
        let Some(facets) = ev
            .raw
            .as_ref()
            .and_then(|r| r.get("run"))
            .and_then(|r| r.get("facets"))
        else {
            return;
        };
        let at = ev
            .event_time
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0));

        // nominalTime → start/end window (RFC3339 strings → instants).
        let (mut nominal_start, mut nominal_end) = (None, None);
        if let Some(nt) = typed::<NominalTimeRunFacet>(facets, "nominalTime") {
            nominal_start = parse_ts(&nt.nominal_start_time);
            nominal_end = parse_ts(&nt.nominal_end_time);
        }

        // parent → parent run id + parent job identity.
        let (mut parent_run_id, mut parent_namespace, mut parent_name) = (None, None, None);
        if let Some(p) = typed::<ParentRunFacet>(facets, "parent") {
            if let Some(r) = p.run.as_option().filter(|r| !r.run_id.is_empty()) {
                parent_run_id = Some(r.run_id.clone());
            }
            if let Some(j) = p.job.as_option() {
                if !j.namespace.is_empty() {
                    parent_namespace = Some(j.namespace.clone());
                }
                if !j.name.is_empty() {
                    parent_name = Some(j.name.clone());
                }
            }
        }

        // errorMessage → the human-readable message.
        let error_message = typed::<ErrorMessageRunFacet>(facets, "errorMessage")
            .map(|e| e.message)
            .filter(|m| !m.is_empty());

        // Nothing relevant → emit nothing.
        if nominal_start.is_none()
            && nominal_end.is_none()
            && parent_run_id.is_none()
            && parent_namespace.is_none()
            && parent_name.is_none()
            && error_message.is_none()
        {
            return;
        }

        out.push(Mutation::SetRunMeta {
            run_id: run_id.clone(),
            at,
            nominal_start,
            nominal_end,
            parent_run_id,
            parent_namespace,
            parent_name,
            error_message,
        });
    }
}

/// Deserialize a named facet from a `facets` object into a typed buffa facet.
fn typed<T: serde::de::DeserializeOwned>(facets: &JsonValue, name: &str) -> Option<T> {
    serde_json::from_value::<T>(facets.get(name)?.clone()).ok()
}

/// Parse an RFC3339 timestamp to UTC; `None` for empty/unparseable.
fn parse_ts(s: &str) -> Option<DateTime<Utc>> {
    if s.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(s).ok().map(|d| d.to_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(facets: JsonValue) -> RawEvent {
        RawEvent {
            seq: 1,
            event_kind: "run".into(),
            event_type: Some("START".into()),
            event_time: DateTime::<Utc>::from_timestamp(1_700_000_000, 0),
            run_id: Some("r1".into()),
            job_namespace: Some("etl".into()),
            job_name: Some("j".into()),
            dataset_namespace: None,
            dataset_name: None,
            raw: Some(json!({ "run": { "runId": "r1", "facets": facets } })),
            inputs: None,
            outputs: None,
            column_lineage: None,
        }
    }

    #[test]
    fn parses_nominal_parent_and_error() {
        let ev = event(json!({
            "nominalTime": {
                "nominalStartTime": "2023-11-14T22:00:00Z",
                "nominalEndTime": "2023-11-14T23:00:00Z"
            },
            "parent": {
                "run": {"runId": "parent-run"},
                "job": {"namespace": "airflow", "name": "dag.task"}
            },
            "errorMessage": {"message": "boom", "programmingLanguage": "PYTHON"}
        }));
        let mut out = Vec::new();
        RunMetaProcessor.process(&ev, &mut out);
        assert_eq!(out.len(), 1);
        match &out[0] {
            Mutation::SetRunMeta {
                nominal_start,
                parent_run_id,
                parent_namespace,
                parent_name,
                error_message,
                ..
            } => {
                assert!(nominal_start.is_some());
                assert_eq!(parent_run_id.as_deref(), Some("parent-run"));
                assert_eq!(parent_namespace.as_deref(), Some("airflow"));
                assert_eq!(parent_name.as_deref(), Some("dag.task"));
                assert_eq!(error_message.as_deref(), Some("boom"));
            }
            other => panic!("expected SetRunMeta, got {other:?}"),
        }
    }

    #[test]
    fn no_relevant_facets_emits_nothing() {
        let ev = event(json!({ "someOtherFacet": {"x": 1} }));
        let mut out = Vec::new();
        RunMetaProcessor.process(&ev, &mut out);
        assert!(out.is_empty());
    }
}
