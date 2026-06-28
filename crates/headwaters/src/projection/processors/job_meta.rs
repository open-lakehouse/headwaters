//! Job-metadata processor: the `sourceCodeLocation`, `jobType`, and `parent`
//! job/run facets → job columns (location, job type, parent job link).
//!
//! `jobType` is parsed via the typed buffa [`JobTypeJobFacet`]; the others are
//! small enough to read from JSON directly (we didn't type
//! `sourceCodeLocation`, and the parent job identity also appears on the run's
//! `parent` facet). Emits a single [`Mutation::SetJobMeta`], latest-wins by `at`.

use crate::lineage::v1::JobTypeJobFacet;
use crate::projection::RawEvent;
use crate::projection::mutation::Mutation;
use crate::projection::processor::FacetProcessor;

pub struct JobMetaProcessor;

impl FacetProcessor for JobMetaProcessor {
    fn name(&self) -> &'static str {
        "jobMeta"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        let (Some(ns), Some(name)) = (&ev.job_namespace, &ev.job_name) else {
            return;
        };
        let Some(raw) = &ev.raw else { return };
        let at = super::core::event_at(ev);

        let job_facets = raw.get("job").and_then(|j| j.get("facets"));

        // sourceCodeLocation.url → job location (untyped facet, read from JSON).
        let location = job_facets
            .and_then(|f| f.get("sourceCodeLocation"))
            .and_then(|s| s.get("url"))
            .and_then(|u| u.as_str())
            .filter(|u| !u.is_empty())
            .map(str::to_string);

        // jobType → a "processingType/integration/jobType" summary string.
        let job_type = job_facets
            .and_then(|f| serde_json::from_value::<JobTypeJobFacet>(f.get("jobType")?.clone()).ok())
            .map(|jt| {
                [jt.processing_type, jt.integration, jt.job_type]
                    .into_iter()
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
                    .join("/")
            })
            .filter(|s| !s.is_empty());

        // parent job identity from the run's `parent` facet.
        let parent = raw
            .get("run")
            .and_then(|r| r.get("facets"))
            .and_then(|f| f.get("parent"))
            .and_then(|p| p.get("job"));
        let parent_namespace = parent
            .and_then(|j| j.get("namespace"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let parent_name = parent
            .and_then(|j| j.get("name"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        if location.is_none()
            && job_type.is_none()
            && parent_namespace.is_none()
            && parent_name.is_none()
        {
            return;
        }

        out.push(Mutation::SetJobMeta {
            namespace: ns.clone(),
            name: name.clone(),
            at,
            location,
            job_type,
            parent_namespace,
            parent_name,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use serde_json::json;

    #[test]
    fn parses_location_jobtype_and_parent() {
        let ev = RawEvent {
            seq: 1,
            event_kind: "run".into(),
            event_type: Some("START".into()),
            event_time: DateTime::<Utc>::from_timestamp(1_700_000_000, 0),
            run_id: Some("r1".into()),
            job_namespace: Some("etl".into()),
            job_name: Some("j".into()),
            dataset_namespace: None,
            dataset_name: None,
            raw: Some(json!({
                "job": {"namespace": "etl", "name": "j", "facets": {
                    "sourceCodeLocation": {"type": "git", "url": "https://git/repo"},
                    "jobType": {"processingType": "BATCH", "integration": "SPARK", "jobType": "QUERY"}
                }},
                "run": {"runId": "r1", "facets": {
                    "parent": {"job": {"namespace": "airflow", "name": "dag.task"}}
                }}
            })),
            inputs: None,
            outputs: None,
            column_lineage: None,
        };
        let mut out = Vec::new();
        JobMetaProcessor.process(&ev, &mut out);
        assert_eq!(out.len(), 1);
        match &out[0] {
            Mutation::SetJobMeta {
                location,
                job_type,
                parent_namespace,
                parent_name,
                ..
            } => {
                assert_eq!(location.as_deref(), Some("https://git/repo"));
                assert_eq!(job_type.as_deref(), Some("BATCH/SPARK/QUERY"));
                assert_eq!(parent_namespace.as_deref(), Some("airflow"));
                assert_eq!(parent_name.as_deref(), Some("dag.task"));
            }
            other => panic!("expected SetJobMeta, got {other:?}"),
        }
    }

    #[test]
    fn no_job_facets_emits_nothing() {
        let ev = RawEvent {
            seq: 1,
            event_kind: "run".into(),
            event_type: Some("START".into()),
            event_time: DateTime::<Utc>::from_timestamp(1_700_000_000, 0),
            run_id: Some("r1".into()),
            job_namespace: Some("etl".into()),
            job_name: Some("j".into()),
            dataset_namespace: None,
            dataset_name: None,
            raw: Some(json!({"job": {"namespace": "etl", "name": "j"}})),
            inputs: None,
            outputs: None,
            column_lineage: None,
        };
        let mut out = Vec::new();
        JobMetaProcessor.process(&ev, &mut out);
        assert!(out.is_empty());
    }
}
