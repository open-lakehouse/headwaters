//! Tags processor: the `tags` job/dataset facets and per-field `schema.tags`
//! → the `tags` catalog and `tag_assignments`.
//!
//! Tags are the seed for downstream tag/PII propagation. They arrive two ways,
//! both handled here identically:
//!   1. on normal lineage events (a producer annotates a dataset/job/field), and
//!   2. on **synthetic "fact discovery" events** — e.g. a scanner emits a
//!      `DatasetEvent` whose `tags` facet asserts "this column is PII". Because
//!      such an event is just an OpenLineage event, it flows through ingest →
//!      `events` → this processor like any other, and the assignment is
//!      rebuildable from the log (see ADR 0017).
//!
//! A tag's name is the facet entry's `key` (e.g. `pii`); `value`/`source` are
//! not modeled as separate assignments — a tag is present or not.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::projection::RawEvent;
use crate::projection::mutation::{Mutation, TagTarget};
use crate::projection::processor::FacetProcessor;

pub struct TagsProcessor;

impl FacetProcessor for TagsProcessor {
    fn name(&self) -> &'static str {
        "tags"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        let Some(raw) = &ev.raw else { return };
        let at = ev
            .event_time
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0));

        // Job-level tags.
        if let (Some(ns), Some(name)) = (&ev.job_namespace, &ev.job_name)
            && let Some(facets) = raw.get("job").and_then(|j| j.get("facets"))
        {
            for tag in tag_keys(facets) {
                push_tag(
                    out,
                    tag,
                    TagTarget::Job {
                        namespace: ns.clone(),
                        name: name.clone(),
                    },
                    at,
                );
            }
        }

        // Dataset-level + field-level tags on every dataset on the event.
        for key in ["inputs", "outputs"] {
            if let Some(arr) = raw.get(key).and_then(|v| v.as_array()) {
                for ds in arr {
                    tags_for_dataset(ds, at, out);
                }
            }
        }
        if let Some(ds) = raw.get("dataset") {
            tags_for_dataset(ds, at, out);
        }
    }
}

fn tags_for_dataset(ds: &JsonValue, at: DateTime<Utc>, out: &mut Vec<Mutation>) {
    let (Some(namespace), Some(name)) = (
        ds.get("namespace").and_then(|v| v.as_str()),
        ds.get("name").and_then(|v| v.as_str()),
    ) else {
        return;
    };
    let Some(facets) = ds.get("facets") else {
        return;
    };

    // Whole-dataset tags (`tags` facet).
    for tag in tag_keys(facets) {
        push_tag(
            out,
            tag,
            TagTarget::Dataset {
                namespace: namespace.to_string(),
                name: name.to_string(),
            },
            at,
        );
    }

    // Field-level tags (`schema` facet's per-field `tags`).
    if let Some(fields) = facets
        .get("schema")
        .and_then(|s| s.get("fields"))
        .and_then(|f| f.as_array())
    {
        for field in fields {
            let Some(field_name) = field.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            for tag in field_tag_keys(field) {
                push_tag(
                    out,
                    tag,
                    TagTarget::DatasetField {
                        namespace: namespace.to_string(),
                        name: name.to_string(),
                        field: field_name.to_string(),
                    },
                    at,
                );
            }
        }
    }
}

/// Emit the catalog entry + the assignment for one tag.
fn push_tag(out: &mut Vec<Mutation>, tag: String, target: TagTarget, at: DateTime<Utc>) {
    out.push(Mutation::UpsertTag {
        tag: tag.clone(),
        description: None,
    });
    out.push(Mutation::TagAssignment { tag, target, at });
}

/// The tag names (entry `key`s) in a `tags` facet on a `facets` object.
fn tag_keys(facets: &JsonValue) -> Vec<String> {
    facets
        .get("tags")
        .and_then(|t| t.get("tags"))
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t.get("key").and_then(|k| k.as_str()).map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The tag names on a schema field. OpenLineage field tags are an array of
/// `{key, ...}` objects (like the facet) or, leniently, plain strings.
fn field_tag_keys(field: &JsonValue) -> Vec<String> {
    field
        .get("tags")
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    t.get("key")
                        .and_then(|k| k.as_str())
                        .or_else(|| t.as_str())
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(raw: JsonValue) -> RawEvent {
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
            raw: Some(raw),
            inputs: None,
            outputs: None,
            column_lineage: None,
        }
    }

    #[test]
    fn parses_dataset_and_field_tags() {
        let ev = event(json!({
            "outputs": [{
                "namespace": "w", "name": "d", "facets": {
                    "tags": {"tags": [{"key": "gold"}]},
                    "schema": {"fields": [
                        {"name": "email", "type": "STRING", "tags": [{"key": "pii"}]},
                        {"name": "id", "type": "BIGINT"}
                    ]}
                }
            }]
        }));
        let mut out = Vec::new();
        TagsProcessor.process(&ev, &mut out);

        let assignments: Vec<(&str, &TagTarget)> = out
            .iter()
            .filter_map(|m| match m {
                Mutation::TagAssignment { tag, target, .. } => Some((tag.as_str(), target)),
                _ => None,
            })
            .collect();
        // Whole-dataset `gold` + field-level `pii` on `email`.
        assert!(assignments.iter().any(|(t, target)| *t == "gold"
            && matches!(target, TagTarget::Dataset { name, .. } if name == "d")));
        assert!(assignments.iter().any(|(t, target)| *t == "pii"
            && matches!(target, TagTarget::DatasetField { field, .. } if field == "email")));
        // Every assignment is paired with a catalog upsert.
        assert_eq!(
            out.iter()
                .filter(|m| matches!(m, Mutation::UpsertTag { .. }))
                .count(),
            assignments.len()
        );
    }

    #[test]
    fn job_tags_become_job_assignments() {
        let ev = event(json!({
            "job": {"namespace": "etl", "name": "j", "facets": {
                "tags": {"tags": [{"key": "critical"}]}
            }}
        }));
        let mut out = Vec::new();
        TagsProcessor.process(&ev, &mut out);
        assert!(out.iter().any(|m| matches!(
            m,
            Mutation::TagAssignment { tag, target: TagTarget::Job { name, .. }, .. }
            if tag == "critical" && name == "j"
        )));
    }

    #[test]
    fn no_tags_emits_nothing() {
        let ev = event(json!({"outputs": [{"namespace": "w", "name": "d"}]}));
        let mut out = Vec::new();
        TagsProcessor.process(&ev, &mut out);
        assert!(out.is_empty());
    }
}
