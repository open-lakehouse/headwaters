//! Direction-aware walking over a [`LineageGraph`].
//!
//! `GetLineage` returns an undirected neighborhood: every reached node carries
//! its incident edges. To answer "what's upstream / downstream of the seed" we
//! walk the edge set from the root in one direction.

use std::collections::HashSet;

use headwaters_client::LineageGraph;

use crate::cli::Direction;

/// An edge `from → to` (data flows from `from` to `to`).
pub struct Edge {
    pub from: String,
    pub to: String,
}

/// All edges in the graph, de-duplicated (each `origin → destination` once).
pub fn edges(graph: &LineageGraph) -> Vec<Edge> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for node in &graph.graph {
        for e in node.out_edges.iter().chain(node.in_edges.iter()) {
            if seen.insert((e.origin.clone(), e.destination.clone())) {
                out.push(Edge {
                    from: e.origin.clone(),
                    to: e.destination.clone(),
                });
            }
        }
    }
    out
}

/// The nodeIds reachable from `root` walking `direction` (BFS over the edge set).
/// `Both` returns every node in the graph. The root is always included.
pub fn reachable(graph: &LineageGraph, root: &str, direction: Direction) -> HashSet<String> {
    let mut keep: HashSet<String> = HashSet::new();
    keep.insert(root.to_string());
    if direction == Direction::Both {
        return graph.graph.iter().map(|n| n.id.clone()).collect();
    }

    let all = edges(graph);
    let mut frontier = vec![root.to_string()];
    while let Some(cur) = frontier.pop() {
        for e in &all {
            let next = match direction {
                Direction::Down if e.from == cur => Some(&e.to),
                Direction::Up if e.to == cur => Some(&e.from),
                _ => None,
            };
            if let Some(n) = next
                && keep.insert(n.clone())
            {
                frontier.push(n.clone());
            }
        }
    }
    keep
}

#[cfg(test)]
mod tests {
    use super::*;
    use headwaters_client::{EntityKind, LineageEdge, LineageNode};

    // raw -> clean(job) -> orders, as the read API returns it (each node carries
    // its incident edges).
    fn linear() -> LineageGraph {
        let pairs = [
            ("dataset:raw:orders", "job:etl:clean"),
            ("job:etl:clean", "dataset:marts:orders"),
        ];
        let edge = |o: &str, d: &str| LineageEdge {
            origin: o.into(),
            destination: d.into(),
            ..Default::default()
        };
        let node = |id: &str, kind: EntityKind| LineageNode {
            id: id.into(),
            r#type: kind.into(),
            in_edges: pairs
                .iter()
                .filter(|(_, d)| *d == id)
                .map(|(o, d)| edge(o, d))
                .collect(),
            out_edges: pairs
                .iter()
                .filter(|(o, _)| *o == id)
                .map(|(o, d)| edge(o, d))
                .collect(),
            ..Default::default()
        };
        LineageGraph {
            graph: vec![
                node("dataset:raw:orders", EntityKind::DATASET),
                node("job:etl:clean", EntityKind::JOB),
                node("dataset:marts:orders", EntityKind::DATASET),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn edges_are_deduped_across_nodes() {
        // Each edge appears on both endpoints' in/out lists; dedup yields 2.
        assert_eq!(edges(&linear()).len(), 2);
    }

    #[test]
    fn upstream_from_sink_reaches_the_source() {
        let keep = reachable(&linear(), "dataset:marts:orders", Direction::Up);
        assert!(keep.contains("dataset:raw:orders"));
        assert!(keep.contains("job:etl:clean"));
        assert_eq!(keep.len(), 3);
    }

    #[test]
    fn downstream_from_source_excludes_nothing_below_but_not_above() {
        let keep = reachable(&linear(), "job:etl:clean", Direction::Down);
        assert!(keep.contains("dataset:marts:orders"));
        assert!(
            !keep.contains("dataset:raw:orders"),
            "raw is upstream of the job"
        );
        assert_eq!(keep.len(), 2);
    }

    #[test]
    fn both_keeps_every_node() {
        assert_eq!(
            reachable(&linear(), "job:etl:clean", Direction::Both).len(),
            3
        );
    }
}
