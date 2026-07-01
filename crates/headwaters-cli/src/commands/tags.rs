//! `hw tags` — list the tag catalog.
//!
//! The governance entry point: an agent runs this to discover which sensitivity
//! labels (`pii`, `gold`, …) exist before asking `hw exposure <tag>`.

use std::io::Write;

use headwaters_client::ListTagsResponse;
use serde_json::{Value, json};

use crate::render::{Render, RenderCtx, table};

/// Renderable wrapper over the list-tags response.
pub struct Tags(pub ListTagsResponse);

impl Render for Tags {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let mut t = table::new(&["TAG", "DESCRIPTION"]);
        for tag in &self.0.tags {
            t.add_row([&tag.name, &tag.description]);
        }
        writeln!(w, "{t}")
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        json!({
            "tags": self.0.tags.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            "count": self.0.tags.len(),
            "_next": self.0.tags.iter().map(|t| format!("hw exposure {}", t.name)).collect::<Vec<_>>(),
        })
    }
}
