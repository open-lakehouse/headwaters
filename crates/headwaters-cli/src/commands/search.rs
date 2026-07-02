//! `hw search <query>` — find jobs and datasets by name.
//!
//! The agent's discovery primitive: turn a fuzzy name into a real `nodeId` that
//! feeds straight into `lineage` / `trace` / `column-lineage`. The `agent`
//! envelope leads with each hit's `node_id` for exactly that.

use std::io::Write;

use headwaters_client::{EntityKind, SearchResponse};
use serde_json::{Value, json};

use crate::render::{Render, RenderCtx, table};

/// Renderable wrapper over the search response.
pub struct SearchView(pub SearchResponse);

impl Render for SearchView {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let mut t = table::new(&["KIND", "NAMESPACE", "NAME", "NODEID"]);
        for r in &self.0.results {
            t.add_row([
                kind_label(r.r#type.as_known()),
                &r.namespace,
                &r.name,
                &r.node_id,
            ]);
        }
        writeln!(w, "{t}")?;
        writeln!(w, "{} results", self.0.total_count)
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        json!({
            "results": self.0.results.iter().map(|r| json!({
                // Lead with the nodeId — it's what the next command needs.
                "node_id": r.node_id,
                "kind": kind_label(r.r#type.as_known()),
                "ref": format!("{}/{}", r.namespace, r.name),
            })).collect::<Vec<_>>(),
            "total": self.0.total_count,
            "_next": self.0.results.iter().take(1).map(|r| {
                format!("hw lineage {}", r.node_id)
            }).collect::<Vec<_>>(),
        })
    }
}

/// The lowercase label for a hit's kind.
fn kind_label(kind: Option<EntityKind>) -> &'static str {
    match kind {
        Some(EntityKind::JOB) => "job",
        Some(EntityKind::DATASET) => "dataset",
        Some(EntityKind::DATASET_FIELD) => "field",
        _ => "unknown",
    }
}
