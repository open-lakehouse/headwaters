//! `hw column-lineage <target>` — how is this column derived?
//!
//! Wraps `GetColumnLineage`, which returns a graph of `datasetField:` nodes with
//! the edges between them. We reshape it into per-output-field derivations: for
//! each field, the upstream input fields it is computed from. This closes the
//! loop from `hw dataset get` / `hw exposure` (which point here) to "…because it
//! derives from that upstream field".

use std::io::Write;

use headwaters_client::{HeadwatersClient, LineageGraph, dataset_field_node_id, struct_to_json};
use serde_json::{Value, json};

use crate::error::CliError;
use crate::graph;
use crate::render::facets::columns_from_fields;
use crate::render::{Render, RenderCtx, table};

/// Fetch the column-lineage graph for a `root` nodeId.
///
/// `GetColumnLineage` only expands a `datasetField:` seed; a bare `dataset:` seed
/// returns an empty graph server-side. So for a dataset target we fan out — fetch
/// the dataset's fields, then union the per-field column-lineage graphs — turning
/// "how are this dataset's columns derived?" into one answer, as the verb promises.
pub async fn fetch(client: &HeadwatersClient, root: &str) -> Result<LineageGraph, CliError> {
    // datasetField seed: one direct call.
    if root.starts_with("datasetField:") {
        return Ok(client.get_column_lineage(root).await?);
    }

    // dataset seed: expand to its fields and union the per-field graphs.
    // `dataset:<ns>:<name>` — split off the kind, the rest is `<ns>:<name>` where
    // the LAST colon separates name (namespaces may themselves contain colons).
    // Only a `dataset:`/`datasetField:` node has columns; reject anything else
    // (e.g. a `job:` nodeId) with a clear error rather than misparsing it into a
    // bogus `get_dataset` call.
    let Some(rest) = root.strip_prefix("dataset:") else {
        return Err(CliError::BadTarget(root.to_string()));
    };
    let Some((namespace, name)) = rest.rsplit_once(':') else {
        return Err(CliError::BadTarget(root.to_string()));
    };

    let dataset = client.get_dataset(namespace, name).await?;
    let fields = columns_from_fields(&struct_to_json_array(&dataset.fields));

    let mut merged: Vec<headwaters_client::LineageNode> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for col in &fields {
        let field_id = dataset_field_node_id(namespace, name, &col.name);
        let graph = client.get_column_lineage(&field_id).await?;
        for node in graph.graph {
            if seen.insert(node.id.clone()) {
                merged.push(node);
            }
        }
    }
    Ok(LineageGraph {
        graph: merged,
        ..Default::default()
    })
}

/// The dataset's `fields` (a `Vec<Struct>`) as a JSON array for `columns_from_fields`.
fn struct_to_json_array(fields: &[headwaters_client::Struct]) -> Value {
    Value::Array(fields.iter().map(struct_to_json).collect())
}

/// A column-lineage graph seeded at one dataset or field.
pub struct ColumnLineageView {
    pub root: String,
    pub graph: LineageGraph,
}

/// One output field and the upstream input fields it derives from.
struct Derivation {
    field: String,
    inputs: Vec<String>,
}

impl ColumnLineageView {
    /// Each field node that has upstream inputs, with those inputs' refs. Fields
    /// with no inputs (pure sources) are omitted — the answer is "what derives
    /// from what".
    fn derivations(&self) -> Vec<Derivation> {
        let edges = graph::edges(&self.graph);
        let mut out: Vec<Derivation> = self
            .graph
            .graph
            .iter()
            .filter_map(|n| {
                let mut inputs: Vec<String> = edges
                    .iter()
                    .filter(|e| e.to == n.id)
                    .map(|e| field_ref(&e.from))
                    .collect();
                inputs.sort();
                inputs.dedup();
                if inputs.is_empty() {
                    return None;
                }
                Some(Derivation {
                    field: field_ref(&n.id),
                    inputs,
                })
            })
            .collect();
        out.sort_by(|a, b| a.field.cmp(&b.field));
        out
    }
}

impl Render for ColumnLineageView {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let derivations = self.derivations();
        writeln!(w, "Column lineage for {}", self.root)?;
        if derivations.is_empty() {
            return writeln!(w, "(no upstream column lineage)");
        }
        let mut t = table::new(&["FIELD", "DERIVES FROM"]);
        for d in &derivations {
            t.add_row([&d.field, &d.inputs.join(", ")]);
        }
        writeln!(w, "{t}")
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.graph).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        let derivations = self.derivations();
        json!({
            "question": format!("column derivation for {}", self.root),
            "root": self.root,
            "fields": derivations.iter().map(|d| json!({
                "field": d.field,
                "derives_from": d.inputs,
            })).collect::<Vec<_>>(),
        })
    }
}

/// A `datasetField:<ns>:<name>:<field>` nodeId → the compact `ns/name:field` ref
/// an agent reads. Peels `field` and `name` off the end so a URI namespace (which
/// itself contains `:`, as in `snowflake://analytics`) stays intact. Falls back to
/// the raw id for any other shape.
fn field_ref(node_id: &str) -> String {
    let Some(rest) = node_id.strip_prefix("datasetField:") else {
        return node_id.to_string();
    };
    // rest = <ns>:<name>:<field>; split the last two colons off the end.
    match rest.rsplitn(3, ':').collect::<Vec<_>>().as_slice() {
        [field, name, ns] => format!("{ns}/{name}:{field}"),
        _ => node_id.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use headwaters_client::{EntityKind, LineageEdge, LineageNode};

    // raw.email -> orders.email (orders.email derives from raw.email).
    fn one_edge() -> LineageGraph {
        let edge = LineageEdge {
            origin: "datasetField:raw:orders:email".into(),
            destination: "datasetField:marts:orders:email".into(),
            ..Default::default()
        };
        let node = |id: &str, in_e: Vec<LineageEdge>, out_e: Vec<LineageEdge>| LineageNode {
            id: id.into(),
            r#type: EntityKind::DATASET_FIELD.into(),
            in_edges: in_e,
            out_edges: out_e,
            ..Default::default()
        };
        LineageGraph {
            graph: vec![
                node("datasetField:raw:orders:email", vec![], vec![edge.clone()]),
                node("datasetField:marts:orders:email", vec![edge], vec![]),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn field_ref_keeps_uri_namespace_intact() {
        // A URI namespace contains `:` — it must not be split.
        assert_eq!(
            field_ref("datasetField:snowflake://analytics:gold.customer_360:email_hash"),
            "snowflake://analytics/gold.customer_360:email_hash"
        );
        assert_eq!(
            field_ref("datasetField:raw:orders:email"),
            "raw/orders:email"
        );
        // Non-field node ids pass through.
        assert_eq!(field_ref("dataset:raw:orders"), "dataset:raw:orders");
    }

    #[test]
    fn reshapes_edges_into_derivations() {
        let view = ColumnLineageView {
            root: "dataset:marts:orders".into(),
            graph: one_edge(),
        };
        let d = view.derivations();
        // Only the output field (with inputs) appears; the pure source is omitted.
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].field, "marts/orders:email");
        assert_eq!(d[0].inputs, vec!["raw/orders:email"]);
    }

    #[test]
    fn agent_envelope_frames_the_question() {
        let view = ColumnLineageView {
            root: "dataset:marts:orders".into(),
            graph: one_edge(),
        };
        let v = view.agent(RenderCtx { raw_facets: false });
        assert_eq!(v["question"], "column derivation for dataset:marts:orders");
        assert_eq!(v["fields"][0]["field"], "marts/orders:email");
    }
}
