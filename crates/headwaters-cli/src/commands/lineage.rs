//! `hw lineage <target>` — the lineage graph around a node.

use std::io::Write;

use headwaters_client::LineageGraph;
use serde_json::{Value, json};

use crate::cli::Direction;
use crate::graph;
use crate::render::{Render, RenderCtx};

/// A lineage graph filtered to one direction from a root node.
pub struct LineageView {
    pub root: String,
    pub direction: Direction,
    pub depth: i32,
    pub graph: LineageGraph,
}

impl LineageView {
    /// The nodes kept after direction-filtering, as `(nodeId, kind, ref)`.
    fn kept_nodes(&self) -> Vec<(String, &'static str, String)> {
        let keep = graph::reachable(&self.graph, &self.root, self.direction);
        self.graph
            .graph
            .iter()
            .filter(|n| keep.contains(&n.id))
            .map(|n| (n.id.clone(), kind_of(&n.id), node_ref(&n.id)))
            .collect()
    }

    fn kept_edges(&self) -> Vec<(String, String)> {
        let keep = graph::reachable(&self.graph, &self.root, self.direction);
        graph::edges(&self.graph)
            .into_iter()
            .filter(|e| keep.contains(&e.from) && keep.contains(&e.to))
            .map(|e| (e.from, e.to))
            .collect()
    }
}

impl Render for LineageView {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let nodes = self.kept_nodes();
        let edges = self.kept_edges();
        writeln!(
            w,
            "{}  (direction: {:?}, depth {})",
            self.root, self.direction, self.depth
        )?;
        for (id, kind, r) in &nodes {
            let marker = if *id == self.root { "*" } else { " " };
            writeln!(w, " {marker} {kind:<8} {r}")?;
        }
        let (jobs, datasets) = count_kinds(&nodes);
        writeln!(
            w,
            "\n{} nodes ({jobs} jobs, {datasets} datasets), {} edges",
            nodes.len(),
            edges.len()
        )
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.graph).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        let nodes = self.kept_nodes();
        let edges = self.kept_edges();
        let (jobs, datasets) = count_kinds(&nodes);
        json!({
            "root": self.root,
            "direction": format!("{:?}", self.direction).to_lowercase(),
            "depth": self.depth,
            // Adjacency only — the full per-node entity blobs are dropped to save
            // tokens; fetch one with `hw dataset get` / a job lookup if needed.
            "nodes": nodes.iter().map(|(id, kind, r)| {
                json!({ "id": id, "kind": kind, "ref": r })
            }).collect::<Vec<_>>(),
            "edges": edges.iter().map(|(f, t)| json!([f, t])).collect::<Vec<_>>(),
            "summary": { "jobs": jobs, "datasets": datasets, "nodes": nodes.len(), "edges": edges.len() },
        })
    }
}

/// `job:ns:name` → `"job"`; `dataset:…` → `"dataset"`; `datasetField:…` → `"field"`.
fn kind_of(node_id: &str) -> &'static str {
    match node_id.split(':').next() {
        Some("job") => "job",
        Some("dataset") => "dataset",
        Some("datasetField") => "field",
        _ => "node",
    }
}

/// Strip the `kind:` prefix for a compact `ns:name` display ref.
fn node_ref(node_id: &str) -> String {
    node_id
        .split_once(':')
        .map(|(_, r)| r.to_string())
        .unwrap_or_else(|| node_id.to_string())
}

fn count_kinds(nodes: &[(String, &'static str, String)]) -> (usize, usize) {
    let jobs = nodes.iter().filter(|(_, k, _)| *k == "job").count();
    let datasets = nodes.iter().filter(|(_, k, _)| *k == "dataset").count();
    (jobs, datasets)
}
