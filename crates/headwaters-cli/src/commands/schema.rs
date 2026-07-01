//! `hw schema` — prime an agent on the data model. No server call.
//!
//! Emits a compact glossary: the nodeId grammar, the entity kinds, run states,
//! the interpreted facets, the `agent` output envelope, and a `commands`
//! capabilities map (each command's question + an example). An agent runs this
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
            "question": "on question-verbs, a one-line restatement of what the answer covers",
            "ref": "the `ns/name` shorthand, for re-addressing",
            "id": "the full nodeId",
            "other_facets": "names of facets present but not interpreted",
            "_next": "suggested follow-up commands (each is runnable as-is)"
        },
        "commands": {
            "hw namespaces": {"q": "what estates exist?", "eg": "hw namespaces"},
            "hw search <q>": {"q": "find a job/dataset by name; get its nodeId", "eg": "hw search orders --kind dataset"},
            "hw dataset list [ns]": {"q": "what datasets are here?", "eg": "hw dataset list analytics"},
            "hw dataset get <ns> <name>": {"q": "what is this dataset (schema, facets)?", "eg": "hw dataset get analytics orders"},
            "hw job list [ns]": {"q": "what jobs are here?", "eg": "hw job list etl"},
            "hw job get <ns> <name>": {"q": "what does this job read/write; latest run?", "eg": "hw job get etl daily"},
            "hw lineage <target>": {"q": "what's around this node?", "eg": "hw lineage dataset:analytics/orders"},
            "hw trace <target> --direction up|down": {"q": "what feeds this / what does it feed?", "eg": "hw trace dataset:analytics/orders --direction up"},
            "hw column-lineage <target>": {"q": "how are this dataset's columns derived?", "eg": "hw column-lineage dataset:analytics/orders"},
            "hw tags": {"q": "what sensitivity labels exist?", "eg": "hw tags"},
            "hw exposure <tag>": {"q": "where does this tag's data end up downstream?", "eg": "hw exposure pii"},
            "hw schema": {"q": "prime on this data model (no server call)", "eg": "hw schema"}
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
