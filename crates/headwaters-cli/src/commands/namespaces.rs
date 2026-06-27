//! `hw namespaces` — list all namespaces.

use std::io::Write;

use headwaters_client::ListNamespacesResponse;
use serde_json::{Value, json};

use crate::render::{Render, RenderCtx, table};

/// Renderable wrapper over the list-namespaces response.
pub struct Namespaces(pub ListNamespacesResponse);

impl Render for Namespaces {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let mut t = table::new(&["NAMESPACE", "OWNER", "DESCRIPTION"]);
        for ns in &self.0.namespaces {
            t.add_row([&ns.name, &ns.owner_name, &ns.description]);
        }
        writeln!(w, "{t}")
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        json!({
            "namespaces": self.0.namespaces.iter().map(|n| n.name.as_str()).collect::<Vec<_>>(),
            "count": self.0.namespaces.len(),
        })
    }
}
