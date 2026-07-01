//! `hw exposure <tag>` — where does this sensitive data end up?
//!
//! The flagship governance verb. Wraps the tag-downstream closure (every field a
//! tag reaches through column lineage; see ADR 0009) and reshapes the flat field
//! list into the answer an agent reasons about: the datasets that expose the tag
//! and, within each, the exposed fields. This is the GDPR data-map / right-to-
//! erasure answer — tag a source column `pii`, ask `hw exposure pii`, get every
//! downstream field to protect or scrub.

use std::io::Write;

use headwaters_client::{TagPropagation, dataset_node_id};
use serde_json::{Value, json};

use crate::render::{Render, RenderCtx, table};

/// A tag's downstream exposure, grouped by dataset.
pub struct Exposure(pub TagPropagation);

/// One exposed dataset: its namespace/name and the fields carrying the tag.
struct ExposedDataset {
    namespace: String,
    name: String,
    fields: Vec<String>,
}

impl ExposedDataset {
    /// The compact `ns/name` display ref.
    fn r#ref(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

impl Exposure {
    /// The reached fields grouped by `namespace/dataset`, both the datasets and
    /// their fields sorted for a stable answer.
    fn by_dataset(&self) -> Vec<ExposedDataset> {
        // Preserve first-seen dataset order via a Vec of (ref, fields); the
        // propagation is small (sparse tag assignments), so linear scan is fine.
        let mut out: Vec<ExposedDataset> = Vec::new();
        for f in &self.0.fields {
            match out
                .iter_mut()
                .find(|d| d.namespace == f.namespace && d.name == f.dataset)
            {
                Some(d) => d.fields.push(f.field.clone()),
                None => out.push(ExposedDataset {
                    namespace: f.namespace.clone(),
                    name: f.dataset.clone(),
                    fields: vec![f.field.clone()],
                }),
            }
        }
        for d in &mut out {
            d.fields.sort();
            d.fields.dedup();
        }
        out.sort_by_key(|a| a.r#ref());
        out
    }
}

impl Render for Exposure {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let datasets = self.by_dataset();
        writeln!(w, "Exposure of tag `{}`", self.0.tag)?;
        if datasets.is_empty() {
            return writeln!(w, "(no downstream fields)");
        }
        let mut t = table::new(&["DATASET", "FIELDS"]);
        for d in &datasets {
            t.add_row([&d.r#ref(), &d.fields.join(", ")]);
        }
        let field_count: usize = datasets.iter().map(|d| d.fields.len()).sum();
        writeln!(w, "{t}")?;
        writeln!(
            w,
            "{} datasets, {field_count} fields expose `{}`",
            datasets.len(),
            self.0.tag
        )
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or(Value::Null)
    }

    fn agent(&self, _ctx: RenderCtx) -> Value {
        let datasets = self.by_dataset();
        let field_count: usize = datasets.iter().map(|d| d.fields.len()).sum();
        // Suggest drilling into the first exposed dataset (if any): fetch it, then
        // see how one of its fields derives.
        let mut next: Vec<String> = Vec::new();
        if let Some(first) = datasets.first() {
            next.push(format!("hw dataset get {} {}", first.namespace, first.name));
            // Emit the canonical `dataset:<ns>:<name>` nodeId (not the `/`-joined
            // ref), so URI namespaces round-trip unambiguously.
            next.push(format!(
                "hw column-lineage {}",
                dataset_node_id(&first.namespace, &first.name)
            ));
        }
        json!({
            "question": format!("downstream exposure of tag {}", self.0.tag),
            "tag": self.0.tag,
            "datasets": datasets.iter().map(|d| json!({
                "ref": d.r#ref(),
                "fields": d.fields,
            })).collect::<Vec<_>>(),
            "dataset_count": datasets.len(),
            "field_count": field_count,
            "_next": next,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use headwaters_client::TaggedField;

    fn field(namespace: &str, dataset: &str, field: &str) -> TaggedField {
        TaggedField {
            namespace: namespace.into(),
            dataset: dataset.into(),
            field: field.into(),
            node_id: format!("datasetField:{namespace}:{dataset}:{field}"),
            ..Default::default()
        }
    }

    fn propagation(fields: Vec<TaggedField>) -> Exposure {
        Exposure(TagPropagation {
            tag: "pii".into(),
            fields,
            ..Default::default()
        })
    }

    #[test]
    fn groups_fields_by_dataset() {
        let e = propagation(vec![
            field("analytics", "orders", "phone"),
            field("analytics", "orders", "email"),
            field("marts", "users", "email"),
        ]);
        let grouped = e.by_dataset();
        assert_eq!(grouped.len(), 2);
        // Sorted by ref: analytics/orders before marts/users.
        assert_eq!(grouped[0].r#ref(), "analytics/orders");
        // Fields within a dataset are sorted.
        assert_eq!(grouped[0].fields, vec!["email", "phone"]);
        assert_eq!(grouped[1].r#ref(), "marts/users");
    }

    #[test]
    fn agent_envelope_counts_and_hints() {
        let e = propagation(vec![
            field("analytics", "orders", "email"),
            field("marts", "users", "email"),
        ]);
        let v = e.agent(RenderCtx { raw_facets: false });
        assert_eq!(v["dataset_count"], 2);
        assert_eq!(v["field_count"], 2);
        assert_eq!(v["question"], "downstream exposure of tag pii");
        // The first drill-in hint targets the first (sorted) exposed dataset.
        assert_eq!(v["_next"][0], "hw dataset get analytics orders");
    }

    #[test]
    fn empty_propagation_has_no_hints() {
        let v = propagation(vec![]).agent(RenderCtx { raw_facets: false });
        assert_eq!(v["dataset_count"], 0);
        assert!(v["_next"].as_array().unwrap().is_empty());
    }
}
