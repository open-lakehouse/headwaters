//! Dataset-metadata processor: the `documentation`, `dataSource`, and
//! `lifecycleStateChange` dataset facets → dataset columns (description,
//! source_name, deleted) and the `sources` catalog.
//!
//! Walks every dataset on the event (`inputs[]`, `outputs[]`, standalone
//! `dataset`). For each, emits a [`Mutation::SetDatasetMeta`] with whatever
//! facets are present, and a [`Mutation::UpsertSource`] when a `dataSource`
//! facet names a source. Latest-wins by event time.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::lineage::v1::{DataSourceDatasetFacet, DocumentationDatasetFacet};
use crate::projection::RawEvent;
use crate::projection::mutation::Mutation;
use crate::projection::processor::FacetProcessor;

pub struct DatasetMetaProcessor;

impl FacetProcessor for DatasetMetaProcessor {
    fn name(&self) -> &'static str {
        "datasetMeta"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        let Some(raw) = &ev.raw else { return };
        let at = ev
            .event_time
            .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0));

        for key in ["inputs", "outputs"] {
            if let Some(arr) = raw.get(key).and_then(|v| v.as_array()) {
                for ds in arr {
                    emit_for(ds, at, out);
                }
            }
        }
        if let Some(ds) = raw.get("dataset") {
            emit_for(ds, at, out);
        }
    }
}

fn emit_for(ds: &JsonValue, at: DateTime<Utc>, out: &mut Vec<Mutation>) {
    let (Some(namespace), Some(name)) = (
        ds.get("namespace").and_then(|v| v.as_str()),
        ds.get("name").and_then(|v| v.as_str()),
    ) else {
        return;
    };
    let Some(facets) = ds.get("facets") else {
        return;
    };

    // documentation → description.
    let description = typed::<DocumentationDatasetFacet>(facets, "documentation")
        .map(|d| d.description)
        .filter(|d| !d.is_empty());

    // dataSource → source name (+ a sources-catalog row).
    let mut source_name = None;
    if let Some(src) =
        typed::<DataSourceDatasetFacet>(facets, "dataSource").filter(|s| !s.name.is_empty())
    {
        source_name = Some(src.name.clone());
        out.push(Mutation::UpsertSource {
            name: src.name,
            connection_url: (!src.uri.is_empty()).then_some(src.uri),
            at,
        });
    }

    // lifecycleStateChange DROP → soft-delete (untyped facet, read from JSON).
    let deleted = facets
        .get("lifecycleStateChange")
        .and_then(|l| l.get("lifecycleStateChange"))
        .and_then(|v| v.as_str())
        .map(|s| s.eq_ignore_ascii_case("DROP"));

    if description.is_none() && source_name.is_none() && deleted.is_none() {
        return;
    }

    out.push(Mutation::SetDatasetMeta {
        namespace: namespace.to_string(),
        name: name.to_string(),
        at,
        description,
        source_name,
        deleted,
    });
}

fn typed<T: serde::de::DeserializeOwned>(facets: &JsonValue, name: &str) -> Option<T> {
    serde_json::from_value::<T>(facets.get(name)?.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(outputs: JsonValue) -> RawEvent {
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
            raw: Some(json!({ "outputs": outputs })),
            inputs: None,
            outputs: None,
            column_lineage: None,
        }
    }

    #[test]
    fn parses_documentation_datasource_and_lifecycle() {
        let ev = event(json!([{
            "namespace": "w", "name": "d", "facets": {
                "documentation": {"description": "the gold table"},
                "dataSource": {"name": "warehouse-db", "uri": "postgres://h/db"},
                "lifecycleStateChange": {"lifecycleStateChange": "DROP"}
            }
        }]));
        let mut out = Vec::new();
        DatasetMetaProcessor.process(&ev, &mut out);

        let source = out.iter().find_map(|m| match m {
            Mutation::UpsertSource {
                name,
                connection_url,
                ..
            } => Some((name.clone(), connection_url.clone())),
            _ => None,
        });
        assert_eq!(
            source,
            Some(("warehouse-db".into(), Some("postgres://h/db".into())))
        );

        let meta = out.iter().find_map(|m| match m {
            Mutation::SetDatasetMeta {
                description,
                source_name,
                deleted,
                ..
            } => Some((description.clone(), source_name.clone(), *deleted)),
            _ => None,
        });
        assert_eq!(
            meta,
            Some((
                Some("the gold table".into()),
                Some("warehouse-db".into()),
                Some(true)
            ))
        );
    }

    #[test]
    fn no_dataset_facets_emits_nothing() {
        let ev = event(json!([{"namespace": "w", "name": "d"}]));
        let mut out = Vec::new();
        DatasetMetaProcessor.process(&ev, &mut out);
        assert!(out.is_empty());
    }
}
