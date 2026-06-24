//! Core table-level processors: the run-state / job-edge / dataset fold that the
//! original `apply_event` performed inline. Split out as pure
//! [`FacetProcessor`]s emitting [`Mutation`]s, with no behavior change.
//!
//! "Table-level" just means these read the promoted columns (`run_id`,
//! `inputs`, `outputs`, `event_type`) and the `documentation`/`tags` job facets,
//! rather than a dedicated facet sub-object — they are ordinary processors.

use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;

use crate::projection::RawEvent;
use crate::projection::mutation::{EntityRef, JobEdges, Mutation};
use crate::projection::processor::FacetProcessor;
use crate::read::model::{dataset_node_id, job_node_id};

/// The event time, or the epoch for an unknown timestamp — matching the old
/// fold, which used `0` so the latest-wins guards still have a total order.
fn event_at(ev: &RawEvent) -> DateTime<Utc> {
    ev.event_time
        .unwrap_or_else(|| DateTime::<Utc>::from_timestamp_nanos(0))
}

// ---------------------------------------------------------------------------
// Run state
// ---------------------------------------------------------------------------

/// Map an OpenLineage `eventType` (case-insensitive) to a Marquez run state.
pub(crate) fn event_type_to_state(et: &str) -> Option<&'static str> {
    match et.to_ascii_uppercase().as_str() {
        "START" | "RUNNING" => Some("RUNNING"),
        "COMPLETE" => Some("COMPLETED"),
        "FAIL" => Some("FAILED"),
        "ABORT" => Some("ABORTED"),
        _ => None,
    }
}

/// Folds a run event's `eventType` + `run_id` into the run's state.
pub struct RunStateProcessor;

impl FacetProcessor for RunStateProcessor {
    fn name(&self) -> &'static str {
        "core.runState"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        if ev.event_kind != "run" && ev.event_kind != "job" {
            return;
        }
        let (Some(job_ns), Some(job_name)) = (&ev.job_namespace, &ev.job_name) else {
            return;
        };
        let Some(run_id) = &ev.run_id else { return };

        let et = ev.event_type.as_deref();
        let state = et.and_then(event_type_to_state);
        let is_start = et.is_some_and(|e| e.eq_ignore_ascii_case("START"));
        let is_terminal = matches!(state, Some("COMPLETED") | Some("FAILED") | Some("ABORTED"));

        out.push(Mutation::UpsertRunState {
            run_id: run_id.clone(),
            job_namespace: job_ns.clone(),
            job_name: job_name.clone(),
            state,
            at: event_at(ev),
            is_start,
            is_terminal,
        });
    }
}

// ---------------------------------------------------------------------------
// Job edges + metadata
// ---------------------------------------------------------------------------

/// Emits the job upsert: namespaces, the job row (edges + documentation/tags
/// metadata), and the directed lineage edges. Datasets implied by the edges are
/// noted by [`DatasetRefProcessor`].
pub struct JobEdgeProcessor;

impl FacetProcessor for JobEdgeProcessor {
    fn name(&self) -> &'static str {
        "core.jobEdges"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        if ev.event_kind != "run" && ev.event_kind != "job" {
            return;
        }
        let (Some(ns), Some(name)) = (&ev.job_namespace, &ev.job_name) else {
            return;
        };
        let at = event_at(ev);

        let in_refs = parse_refs(&ev.inputs);
        let out_refs = parse_refs(&ev.outputs);
        let carries_edges = !in_refs.is_empty() || !out_refs.is_empty();
        let (description, tags) = parse_job_meta(&ev.raw);
        let has_meta = description.is_some() || !tags.is_empty();

        out.push(Mutation::NoteNamespace {
            name: ns.clone(),
            at,
        });

        out.push(Mutation::UpsertJob {
            namespace: ns.clone(),
            name: name.clone(),
            at,
            edges: carries_edges.then(|| JobEdges {
                inputs: in_refs.clone(),
                outputs: out_refs.clone(),
            }),
            description,
            tags: has_meta.then_some(tags),
        });

        // Directed edges: input dataset -> job, job -> output dataset.
        if carries_edges {
            let job_node = job_node_id(ns, name);
            for r in &in_refs {
                out.push(Mutation::UpsertLineageEdge {
                    origin: dataset_node_id(&r.namespace, &r.name),
                    destination: job_node.clone(),
                });
            }
            for r in &out_refs {
                out.push(Mutation::UpsertLineageEdge {
                    origin: job_node.clone(),
                    destination: dataset_node_id(&r.namespace, &r.name),
                });
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Datasets implied by edges or standalone dataset events
// ---------------------------------------------------------------------------

/// Notes datasets: those implied by a job's input/output edges (output datasets
/// may carry a `schema` facet → fields) and standalone `dataset` events.
pub struct DatasetRefProcessor;

impl FacetProcessor for DatasetRefProcessor {
    fn name(&self) -> &'static str {
        "core.datasetRefs"
    }

    fn process(&self, ev: &RawEvent, out: &mut Vec<Mutation>) {
        let at = event_at(ev);
        match ev.event_kind.as_str() {
            "run" | "job" => {
                let in_refs = parse_refs(&ev.inputs);
                let out_refs = parse_refs(&ev.outputs);
                let out_schemas = parse_output_schemas(&ev.raw);
                for r in &in_refs {
                    out.push(Mutation::NoteNamespace {
                        name: r.namespace.clone(),
                        at,
                    });
                    out.push(Mutation::UpsertDataset {
                        namespace: r.namespace.clone(),
                        name: r.name.clone(),
                        at,
                        fields: None,
                    });
                }
                for r in &out_refs {
                    out.push(Mutation::NoteNamespace {
                        name: r.namespace.clone(),
                        at,
                    });
                    out.push(Mutation::UpsertDataset {
                        namespace: r.namespace.clone(),
                        name: r.name.clone(),
                        at,
                        fields: out_schemas
                            .get(&(r.namespace.clone(), r.name.clone()))
                            .cloned(),
                    });
                }
            }
            "dataset" => {
                if let (Some(ns), Some(name)) = (&ev.dataset_namespace, &ev.dataset_name) {
                    out.push(Mutation::UpsertDataset {
                        namespace: ns.clone(),
                        name: name.clone(),
                        at,
                        fields: None,
                    });
                }
            }
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Shared parse helpers (ported verbatim from the old apply.rs)
// ---------------------------------------------------------------------------

/// Parse a `[{namespace,name}]` JSON array of dataset references.
pub(crate) fn parse_refs(val: &Option<JsonValue>) -> Vec<EntityRef> {
    let Some(JsonValue::Array(arr)) = val else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|v| {
            Some(EntityRef {
                namespace: v.get("namespace")?.as_str()?.to_string(),
                name: v.get("name")?.as_str()?.to_string(),
            })
        })
        .collect()
}

/// Extract the job description (`documentation` job facet) and tags (`tags` job
/// facet, rendered as `key` / `key:value`) from an event's raw JSON.
pub(crate) fn parse_job_meta(raw: &Option<JsonValue>) -> (Option<String>, Vec<String>) {
    let Some(event) = raw else {
        return (None, Vec::new());
    };
    let Some(facets) = event.get("job").and_then(|j| j.get("facets")) else {
        return (None, Vec::new());
    };

    let description = facets
        .get("documentation")
        .and_then(|d| d.get("description"))
        .and_then(|d| d.as_str())
        .map(str::to_string);

    let tags = facets
        .get("tags")
        .and_then(|t| t.get("tags"))
        .and_then(|t| t.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|t| {
                    let key = t.get("key")?.as_str()?;
                    Some(match t.get("value").and_then(|v| v.as_str()) {
                        Some(value) => format!("{key}:{value}"),
                        None => key.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    (description, tags)
}

/// Extract per-output-dataset schema fields from an event's raw JSON:
/// `outputs[].facets.schema.fields`, keyed by `(namespace, name)`.
pub(crate) fn parse_output_schemas(
    raw: &Option<JsonValue>,
) -> std::collections::HashMap<(String, String), Vec<JsonValue>> {
    let mut out = std::collections::HashMap::new();
    let Some(event) = raw else { return out };
    let Some(outputs) = event.get("outputs").and_then(|o| o.as_array()) else {
        return out;
    };
    for ds in outputs {
        let (Some(ns), Some(name)) = (
            ds.get("namespace").and_then(|v| v.as_str()),
            ds.get("name").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        if let Some(fields) = ds
            .get("facets")
            .and_then(|f| f.get("schema"))
            .and_then(|s| s.get("fields"))
            .and_then(|f| f.as_array())
        {
            out.insert((ns.to_string(), name.to_string()), fields.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    //! DB-free processor tests: assert `RawEvent -> Vec<Mutation>` directly,
    //! no Postgres. This is the fast-feedback layer the mutation IR unlocks.
    use super::*;
    use serde_json::json;

    fn at() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap()
    }

    /// A run event reading `raw.orders` and writing `marts.daily` with a runId.
    fn run_event() -> RawEvent {
        RawEvent {
            seq: 1,
            event_kind: "run".into(),
            event_type: Some("COMPLETE".into()),
            event_time: Some(at()),
            run_id: Some("run-1".into()),
            job_namespace: Some("etl".into()),
            job_name: Some("build_daily".into()),
            dataset_namespace: None,
            dataset_name: None,
            raw: None,
            inputs: Some(json!([{"namespace": "raw", "name": "orders"}])),
            outputs: Some(json!([{"namespace": "marts", "name": "daily"}])),
            column_lineage: None,
        }
    }

    fn collect(p: &dyn FacetProcessor, ev: &RawEvent) -> Vec<Mutation> {
        let mut out = Vec::new();
        p.process(ev, &mut out);
        out
    }

    #[test]
    fn run_state_maps_complete_to_completed() {
        let muts = collect(&RunStateProcessor, &run_event());
        assert_eq!(muts.len(), 1);
        match &muts[0] {
            Mutation::UpsertRunState {
                run_id,
                state,
                is_terminal,
                is_start,
                ..
            } => {
                assert_eq!(run_id, "run-1");
                assert_eq!(*state, Some("COMPLETED"));
                assert!(is_terminal);
                assert!(!is_start);
            }
            other => panic!("expected UpsertRunState, got {other:?}"),
        }
    }

    #[test]
    fn run_state_skips_event_without_run_id() {
        let mut ev = run_event();
        ev.run_id = None;
        assert!(collect(&RunStateProcessor, &ev).is_empty());
    }

    #[test]
    fn job_edges_emit_namespace_job_and_directed_edges() {
        let muts = collect(&JobEdgeProcessor, &run_event());
        // NoteNamespace(etl) + UpsertJob + 1 input edge + 1 output edge.
        assert!(matches!(&muts[0], Mutation::NoteNamespace { name, .. } if name == "etl"));
        let job = muts
            .iter()
            .find_map(|m| match m {
                Mutation::UpsertJob { edges, .. } => Some(edges.clone()),
                _ => None,
            })
            .expect("UpsertJob present");
        let edges = job.expect("edges carried");
        assert_eq!(edges.inputs.len(), 1);
        assert_eq!(edges.inputs[0].namespace, "raw");
        assert_eq!(edges.outputs[0].name, "daily");

        let lineage: Vec<_> = muts
            .iter()
            .filter_map(|m| match m {
                Mutation::UpsertLineageEdge {
                    origin,
                    destination,
                } => Some((origin.as_str(), destination.as_str())),
                _ => None,
            })
            .collect();
        assert!(lineage.contains(&("dataset:raw:orders", "job:etl:build_daily")));
        assert!(lineage.contains(&("job:etl:build_daily", "dataset:marts:daily")));
    }

    #[test]
    fn empty_terminal_carries_no_edges() {
        // A COMPLETE that drops the datasets emits an UpsertJob with edges=None,
        // so the applier preserves the stored edges (the edge-union guarantee).
        let mut ev = run_event();
        ev.inputs = None;
        ev.outputs = None;
        let muts = collect(&JobEdgeProcessor, &ev);
        let edges = muts.iter().find_map(|m| match m {
            Mutation::UpsertJob { edges, .. } => Some(edges.clone()),
            _ => None,
        });
        assert_eq!(edges, Some(None), "no edges carried -> edges: None");
        assert!(
            !muts
                .iter()
                .any(|m| matches!(m, Mutation::UpsertLineageEdge { .. })),
            "no lineage edges emitted"
        );
    }

    #[test]
    fn job_meta_parsed_from_documentation_and_tags_facets() {
        let mut ev = run_event();
        ev.raw = Some(json!({
            "job": {"namespace": "etl", "name": "build_daily", "facets": {
                "documentation": {"description": "Daily rollup."},
                "tags": {"tags": [{"key": "tier", "value": "bronze"}, {"key": "adhoc"}]}
            }}
        }));
        let muts = collect(&JobEdgeProcessor, &ev);
        match muts
            .iter()
            .find(|m| matches!(m, Mutation::UpsertJob { .. }))
        {
            Some(Mutation::UpsertJob {
                description, tags, ..
            }) => {
                assert_eq!(description.as_deref(), Some("Daily rollup."));
                assert_eq!(
                    tags.clone().unwrap(),
                    vec!["tier:bronze".to_string(), "adhoc".to_string()]
                );
            }
            _ => panic!("expected UpsertJob"),
        }
    }

    #[test]
    fn dataset_refs_note_inputs_and_outputs_with_output_schema() {
        let mut ev = run_event();
        ev.raw = Some(json!({
            "outputs": [{"namespace": "marts", "name": "daily", "facets": {
                "schema": {"fields": [{"name": "id", "type": "BIGINT"}]}
            }}]
        }));
        let muts = collect(&DatasetRefProcessor, &ev);
        let daily = muts.iter().find_map(|m| match m {
            Mutation::UpsertDataset {
                namespace,
                name,
                fields,
                ..
            } if namespace == "marts" && name == "daily" => Some(fields.clone()),
            _ => None,
        });
        let fields = daily
            .expect("output dataset noted")
            .expect("schema fields lifted");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0]["name"], "id");
        // The input dataset is noted without a schema.
        assert!(muts.iter().any(|m| matches!(
            m,
            Mutation::UpsertDataset { namespace, name, fields: None, .. }
            if namespace == "raw" && name == "orders"
        )));
    }
}
