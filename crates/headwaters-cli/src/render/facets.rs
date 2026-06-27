//! Interpret the open-ended OpenLineage facet bag into plain, named fields.
//!
//! Datasets and runs carry a `facets` object keyed by facet name; jobs carry
//! their facets inside the lineage-node `data`. The high-value facets have a
//! known shape, so we lift them into flat fields the `table` and `agent`
//! renderers show directly, instead of making a human (or an LLM) re-parse the
//! raw JSON. Unknown facets are summarized by name (see [`other_facet_names`]).

use serde_json::Value;

/// A schema column lifted from the `schema` facet (or a dataset's `fields`).
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub r#type: String,
    pub description: Option<String>,
}

/// Facet keys we interpret; everything else is "other".
const KNOWN_FACETS: &[&str] = &[
    "schema",
    "columnLineage",
    "sql",
    "documentation",
    "dataSource",
    "jobType",
];

/// Extract schema columns from a dataset's `fields` array (each entry an object
/// like `{name, type, description}`). Tolerant of missing keys.
pub fn columns_from_fields(fields: &Value) -> Vec<Column> {
    fields
        .as_array()
        .map(|arr| arr.iter().filter_map(column_from_obj).collect())
        .unwrap_or_default()
}

/// Extract columns from a `schema` facet object (`{ fields: [...] }`).
pub fn columns_from_schema_facet(facets: &Value) -> Vec<Column> {
    facets
        .get("schema")
        .and_then(|s| s.get("fields"))
        .map(columns_from_fields)
        .unwrap_or_default()
}

fn column_from_obj(v: &Value) -> Option<Column> {
    let name = v.get("name")?.as_str()?.to_string();
    Some(Column {
        name,
        r#type: v
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string(),
        description: v
            .get("description")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
    })
}

/// The SQL query from a `sql` facet, if present.
pub fn sql(facets: &Value) -> Option<String> {
    facets
        .get("sql")
        .and_then(|s| s.get("query"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// The names of facets present but not interpreted — so an agent knows they
/// exist without paying for their bytes.
pub fn other_facet_names(facets: &Value) -> Vec<String> {
    facets
        .as_object()
        .map(|m| {
            m.keys()
                .filter(|k| !KNOWN_FACETS.contains(&k.as_str()))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}
