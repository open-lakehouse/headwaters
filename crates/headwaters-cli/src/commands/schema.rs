//! `hw schema` — prime an agent on the data model. No server call.
//!
//! Emits a compact glossary: the nodeId grammar, the entity kinds, run states,
//! the interpreted facets, and the `agent` output envelope. An agent runs this
//! once and then never has to re-derive the model from individual responses.

use std::io::Write;

use serde_json::{Value, json};

use crate::render::{Render, RenderCtx};

/// The static data-model description.
pub struct Schema;

fn doc() -> Value {
    json!({
        "_v": 1,
        "about": "Headwaters tracks data lineage: jobs read input datasets and write \
                  output datasets, forming a directed graph. `hw` inspects it.",
        "node_id": {
            "grammar": "<kind>:<namespace>:<name> | datasetField:<namespace>:<name>:<field>",
            "examples": ["job:etl:daily", "dataset:analytics:orders", "datasetField:analytics:orders:email"],
            "shorthand": "commands also accept `kind:<ns>/<name>` (slash splits ns from name)"
        },
        "entity_kinds": {
            "JOB": "a recurring process that reads and writes datasets",
            "DATASET": "a table/file/stream produced or consumed by jobs",
            "DATASET_FIELD": "a single column (column-lineage graphs only)"
        },
        "run_states": ["NEW", "RUNNING", "COMPLETED", "FAILED", "ABORTED"],
        "interpreted_facets": {
            "schema": "-> columns[] {name, type, description}",
            "sql": "-> sql (the query the job ran)",
            "columnLineage": "-> per-output-field input derivation",
            "documentation": "-> description"
        },
        "output_modes": {
            "table": "human-readable",
            "json": "faithful wire message (stable; for scripts)",
            "agent": "this envelope style — interpreted, pruned, with `_next` hints"
        },
        "agent_envelope": {
            "ref": "the `ns/name` shorthand, for re-addressing",
            "id": "the full nodeId",
            "other_facets": "names of facets present but not interpreted",
            "_next": "suggested follow-up commands"
        }
    })
}

impl Render for Schema {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        // The glossary is inherently structured; pretty-print the JSON for humans too.
        writeln!(
            w,
            "{}",
            serde_json::to_string_pretty(&doc()).unwrap_or_default()
        )
    }

    fn json(&self) -> Value {
        doc()
    }
}
