//! Build the `nodeId` strings the lineage graph addresses nodes by.
//!
//! A nodeId is `<kind>:<namespace>:<name>` for jobs and datasets, or
//! `datasetField:<namespace>:<name>:<field>` for a column. Pass the result to
//! [`get_lineage`](crate::HeadwatersClient::get_lineage) /
//! [`get_column_lineage`](crate::HeadwatersClient::get_column_lineage), or read
//! one off a [`SearchResult`](crate::SearchResult) / [`LineageNode`](crate::LineageNode).

/// The nodeId for a job: `job:<namespace>:<name>`.
pub fn job_node_id(namespace: &str, name: &str) -> String {
    format!("job:{namespace}:{name}")
}

/// The nodeId for a dataset: `dataset:<namespace>:<name>`.
pub fn dataset_node_id(namespace: &str, name: &str) -> String {
    format!("dataset:{namespace}:{name}")
}

/// The nodeId for a dataset field: `datasetField:<namespace>:<dataset>:<field>`.
pub fn dataset_field_node_id(namespace: &str, dataset: &str, field: &str) -> String {
    format!("datasetField:{namespace}:{dataset}:{field}")
}
