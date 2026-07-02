//! `hw job list` / `hw job get` — inspect jobs.
//!
//! A job is a recurring process that reads input datasets and writes outputs.
//! `get` surfaces its inputs/outputs and latest run state; `list` pages the
//! catalog. Both round out the `job` half of the resource+verb grid.

use std::io::Write;

use headwaters_client::{JobDetail, ListJobsResponse, RunDetail, RunState, job_node_id};
use serde_json::{Value, json};

use crate::render::{Render, RenderCtx, table};

/// Renderable wrapper over a paged job list.
pub struct JobList(pub ListJobsResponse);

impl Render for JobList {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let mut t = table::new(&["NAMESPACE", "NAME", "LATEST RUN"]);
        for j in &self.0.jobs {
            t.add_row([&j.namespace, &j.name, latest_state(j)]);
        }
        writeln!(w, "{t}")?;
        writeln!(w, "{} of {} jobs", self.0.jobs.len(), self.0.total_count)
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        json!({
            "jobs": self.0.jobs.iter().map(|j| json!({
                "id": job_node_id(&j.namespace, &j.name),
                "ref": format!("{}/{}", j.namespace, j.name),
                "latest_run": latest_state(j),
            })).collect::<Vec<_>>(),
            "count": self.0.jobs.len(),
            "total": self.0.total_count,
        })
    }
}

/// Renderable wrapper over one job.
pub struct JobView(pub JobDetail);

impl JobView {
    fn refs(entities: &[headwaters_client::EntityId]) -> Vec<String> {
        entities
            .iter()
            .map(|e| format!("{}/{}", e.namespace, e.name))
            .collect()
    }
}

impl Render for JobView {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let j = &self.0;
        writeln!(w, "Job      {}/{}", j.namespace, j.name)?;
        if !j.description.is_empty() {
            writeln!(w, "About    {}", j.description)?;
        }
        if !j.location.is_empty() {
            writeln!(w, "Location {}", j.location)?;
        }
        writeln!(w, "Inputs   {}", join_or_none(&Self::refs(&j.inputs)))?;
        writeln!(w, "Outputs  {}", join_or_none(&Self::refs(&j.outputs)))?;
        if let Some(run) = j.latest_run.as_option() {
            writeln!(w, "Latest   {} ({})", state_label(run), run.id)?;
        }
        Ok(())
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        let j = &self.0;
        let mut out = json!({
            "kind": "job",
            "id": job_node_id(&j.namespace, &j.name),
            "ref": format!("{}/{}", j.namespace, j.name),
            "inputs": Self::refs(&j.inputs),
            "outputs": Self::refs(&j.outputs),
        });
        let map = out.as_object_mut().expect("object");
        if !j.description.is_empty() {
            map.insert("description".into(), json!(j.description));
        }
        if let Some(run) = j.latest_run.as_option() {
            map.insert(
                "latest_run".into(),
                json!({ "id": run.id, "state": state_label(run) }),
            );
        }
        // Canonical `job:<ns>:<name>` nodeId so URI namespaces round-trip.
        let id = job_node_id(&j.namespace, &j.name);
        map.insert(
            "_next".into(),
            json!([
                format!("hw lineage {id} --direction down"),
                format!("hw trace {id} --direction up"),
            ]),
        );
        out
    }
}

/// The latest run's state label, or `"-"` when the job has never run.
fn latest_state(j: &JobDetail) -> &'static str {
    j.latest_run.as_option().map(state_label).unwrap_or("-")
}

/// The lowercase state name of a run.
fn state_label(run: &RunDetail) -> &'static str {
    match run.state.as_known() {
        Some(RunState::NEW) => "new",
        Some(RunState::RUNNING) => "running",
        Some(RunState::COMPLETED) => "completed",
        Some(RunState::FAILED) => "failed",
        Some(RunState::ABORTED) => "aborted",
        _ => "unknown",
    }
}

fn join_or_none(items: &[String]) -> String {
    if items.is_empty() {
        "(none)".to_string()
    } else {
        items.join(", ")
    }
}
