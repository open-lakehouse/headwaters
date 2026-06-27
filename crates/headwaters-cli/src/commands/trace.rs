//! `hw trace <target> --direction up|down` — a semantic verb.
//!
//! Reshapes the lineage graph into the *answer* to "what feeds this / what does
//! this feed": the source (or sink) datasets, the transforming jobs between, and
//! the node count. This is the shape an agent reasons about, not a raw
//! neighborhood dump.

use std::io::Write;

use headwaters_client::LineageGraph;
use serde_json::{Value, json};

use crate::cli::Direction;
use crate::graph;
use crate::render::{Render, RenderCtx};

/// A directional trace from a root node.
pub struct TraceView {
    pub root: String,
    pub direction: Direction,
    pub graph: LineageGraph,
}

impl TraceView {
    fn parts(&self) -> (Vec<String>, Vec<String>) {
        let keep = graph::reachable(&self.graph, &self.root, self.direction);
        let edges = graph::edges(&self.graph);

        // Transforming jobs: every kept job node.
        let mut jobs: Vec<String> = keep
            .iter()
            .filter(|id| id.starts_with("job:"))
            .map(|id| node_ref(id))
            .collect();
        jobs.sort();

        // Endpoints: kept datasets that are sources (no kept inbound edge) when
        // tracing up, or sinks (no kept outbound edge) when tracing down.
        let mut endpoints: Vec<String> = keep
            .iter()
            .filter(|id| id.starts_with("dataset:") && *id != &self.root)
            .filter(|id| is_endpoint(id, &edges, &keep, self.direction))
            .map(|id| node_ref(id))
            .collect();
        endpoints.sort();
        endpoints.dedup();

        (endpoints, jobs)
    }
}

impl Render for TraceView {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let (endpoints, jobs) = self.parts();
        let label = if self.direction == Direction::Up {
            "Sources"
        } else {
            "Sinks"
        };
        writeln!(w, "Trace {:?} of {}", self.direction, self.root)?;
        writeln!(w, "{label}: {}", join_or_none(&endpoints))?;
        writeln!(w, "Via jobs: {}", join_or_none(&jobs))
    }

    fn json(&self) -> Value {
        self.agent(RenderCtx { raw_facets: false })
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        let (endpoints, jobs) = self.parts();
        let dir = if self.direction == Direction::Up {
            "upstream"
        } else {
            "downstream"
        };
        let endpoint_key = if self.direction == Direction::Up {
            "sources"
        } else {
            "sinks"
        };
        json!({
            "question": format!("{dir} of {}", self.root),
            "root": self.root,
            "direction": dir,
            endpoint_key: endpoints,
            "transforming_jobs": jobs,
        })
    }
}

/// A dataset is an endpoint if it has no kept edge continuing in the trace
/// direction (a source has no kept inbound edge; a sink no kept outbound).
fn is_endpoint(
    id: &str,
    edges: &[graph::Edge],
    keep: &std::collections::HashSet<String>,
    direction: Direction,
) -> bool {
    !edges.iter().any(|e| match direction {
        Direction::Up => e.to == id && keep.contains(&e.from),
        Direction::Down => e.from == id && keep.contains(&e.to),
        Direction::Both => false,
    })
}

fn node_ref(node_id: &str) -> String {
    node_id
        .split_once(':')
        .map(|(_, r)| r.to_string())
        .unwrap_or_else(|| node_id.to_string())
}

fn join_or_none(items: &[String]) -> String {
    if items.is_empty() {
        "(none)".to_string()
    } else {
        items.join(", ")
    }
}
