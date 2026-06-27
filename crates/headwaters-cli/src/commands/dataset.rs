//! `hw dataset get <ns> <name>` — one dataset, with its schema and facets.

use std::io::Write;

use headwaters_client::{Dataset, dataset_node_id, struct_to_json};
use serde_json::{Value, json};

use crate::render::facets::{self, Column, columns_from_fields};
use crate::render::{Render, RenderCtx, table};

/// Renderable wrapper over a dataset.
pub struct DatasetView(pub Dataset);

impl DatasetView {
    /// Schema columns, from the dataset's `fields` (always present) merged with
    /// anything only in the `schema` facet.
    fn columns(&self) -> Vec<Column> {
        let from_fields: Vec<Column> = self
            .0
            .fields
            .iter()
            .map(struct_to_json)
            .collect::<Vec<_>>()
            .iter()
            .flat_map(|v| columns_from_fields(&json!([v])))
            .collect();
        if !from_fields.is_empty() {
            return from_fields;
        }
        facets::columns_from_schema_facet(&self.facets_json())
    }

    fn facets_json(&self) -> Value {
        self.0
            .facets
            .as_option()
            .map(struct_to_json)
            .unwrap_or(Value::Null)
    }
}

impl Render for DatasetView {
    fn table(&self, w: &mut dyn Write, _ctx: RenderCtx) -> std::io::Result<()> {
        let d = &self.0;
        writeln!(w, "Dataset  {}/{}", d.namespace, d.name)?;
        if !d.physical_name.is_empty() {
            writeln!(w, "Physical {}", d.physical_name)?;
        }
        if !d.source_name.is_empty() {
            writeln!(w, "Source   {}", d.source_name)?;
        }
        if !d.description.is_empty() {
            writeln!(w, "About    {}", d.description)?;
        }
        if !d.tags.is_empty() {
            writeln!(w, "Tags     {}", d.tags.join(", "))?;
        }
        let cols = self.columns();
        if !cols.is_empty() {
            writeln!(w, "\nColumns ({})", cols.len())?;
            let mut t = table::new(&["NAME", "TYPE", "DESCRIPTION"]);
            for c in &cols {
                t.add_row([&c.name, &c.r#type, c.description.as_deref().unwrap_or("")]);
            }
            writeln!(w, "{t}")?;
        }
        Ok(())
    }

    fn json(&self) -> Value {
        serde_json::to_value(&self.0).unwrap_or(Value::Null)
    }

    fn agent(&self, ctx: RenderCtx) -> Value {
        let d = &self.0;
        let mut out = json!({
            "kind": "dataset",
            "id": dataset_node_id(&d.namespace, &d.name),
            "ref": format!("{}/{}", d.namespace, d.name),
            "namespace": d.namespace,
            "name": d.name,
        });
        let map = out.as_object_mut().expect("object");
        if !d.description.is_empty() {
            map.insert("description".into(), json!(d.description));
        }
        if !d.tags.is_empty() {
            map.insert("tags".into(), json!(d.tags));
        }
        let cols = self.columns();
        if !cols.is_empty() {
            map.insert(
                "columns".into(),
                Value::Array(
                    cols.iter()
                        .map(|c| {
                            let mut o = json!({ "name": c.name, "type": c.r#type });
                            if let Some(desc) = &c.description {
                                o.as_object_mut()
                                    .unwrap()
                                    .insert("description".into(), json!(desc));
                            }
                            o
                        })
                        .collect(),
                ),
            );
        }
        let fj = self.facets_json();
        if ctx.raw_facets {
            map.insert("facets".into(), fj);
        } else {
            if let Some(sql) = facets::sql(&fj) {
                map.insert("sql".into(), json!(sql));
            }
            let others = facets::other_facet_names(&fj);
            if !others.is_empty() {
                map.insert("other_facets".into(), json!(others));
            }
        }
        map.insert(
            "_next".into(),
            json!([
                format!(
                    "hw lineage dataset:{}/{} --direction down",
                    d.namespace, d.name
                ),
                format!("hw column-lineage dataset:{}/{}", d.namespace, d.name),
            ]),
        );
        out
    }
}
