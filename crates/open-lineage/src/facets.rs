//! OpenLineage facet types.
//!
//! Every facet embeds a [`BaseFacet`] carrying the spec-mandated `_producer` and
//! `_schemaURL` fields (underscore-prefixed to avoid collisions with the facet
//! payload). Shapes follow the OpenLineage spec; see
//! <https://openlineage.io/docs/spec/facets/>.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Common fields present on every OpenLineage facet.
///
/// Flattened into each concrete facet so the serialized JSON carries the
/// required `_producer` and `_schemaURL` keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
}

impl BaseFacet {
    /// Build a [`BaseFacet`] for `producer` pointing at the facet schema named
    /// `schema` (e.g. `1-2-0/SchemaDatasetFacet.json`).
    pub fn new(producer: &str, schema: &str) -> Self {
        Self {
            producer: producer.to_string(),
            schema_url: format!("https://openlineage.io/spec/facets/{schema}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Run facets
// ---------------------------------------------------------------------------

/// Bag of run facets. Known facets are typed; anything supplied by a
/// [`crate::context::LineageContext`] flows through `extra`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processing_engine: Option<ProcessingEngineRunFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<ParentRunFacet>,
    #[serde(rename = "nominalTime", skip_serializing_if = "Option::is_none")]
    pub nominal_time: Option<NominalTimeRunFacet>,
    #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<ErrorMessageRunFacet>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// The facet a query engine is expected to populate to identify itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingEngineRunFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub version: String,
    pub name: String,
    #[serde(rename = "openlineageAdapterVersion")]
    pub openlineage_adapter_version: String,
}

/// Links this run to a parent (and optionally root) run/job — set by orchestrators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentRunFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub run: ParentRun,
    pub job: ParentJob,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<RootParent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentRun {
    #[serde(rename = "runId")]
    pub run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentJob {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootParent {
    pub run: ParentRun,
    pub job: ParentJob,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NominalTimeRunFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    #[serde(rename = "nominalStartTime")]
    pub nominal_start_time: String,
    #[serde(rename = "nominalEndTime", skip_serializing_if = "Option::is_none")]
    pub nominal_end_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessageRunFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub message: String,
    #[serde(rename = "programmingLanguage")]
    pub programming_language: String,
    #[serde(rename = "stackTrace", skip_serializing_if = "Option::is_none")]
    pub stack_trace: Option<String>,
}

// ---------------------------------------------------------------------------
// Job facets
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql: Option<SqlJobFacet>,
    #[serde(rename = "jobType", skip_serializing_if = "Option::is_none")]
    pub job_type: Option<JobTypeJobFacet>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlJobFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTypeJobFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    #[serde(rename = "processingType")]
    pub processing_type: String,
    pub integration: String,
    #[serde(rename = "jobType")]
    pub job_type: String,
}

/// Free-text description of a job (the `documentation` job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentationJobFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub description: String,
}

/// Who owns a job (the `ownership` job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnershipJobFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub owners: Vec<Owner>,
}

/// One owner entry of an [`OwnershipJobFacet`]: a name plus an optional kind
/// (e.g. `MAINTAINER`, or a custom value like `team` / `user`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Owner {
    pub name: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<String>,
}

/// Business tags on a job (the `tags` job facet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagsJobFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub tags: Vec<TagsJobFacetFields>,
}

/// One tag of a [`TagsJobFacet`]: a key with an optional value and an optional
/// source naming the system that assigned the tag.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagsJobFacetFields {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

// ---------------------------------------------------------------------------
// Dataset facets
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DatasetFacets {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<SchemaDatasetFacet>,
    #[serde(rename = "dataSource", skip_serializing_if = "Option::is_none")]
    pub data_source: Option<DataSourceDatasetFacet>,
    /// Column-level lineage. Per spec this facet describes how an **output**
    /// dataset's fields are derived, so it is only ever attached to outputs.
    #[serde(rename = "columnLineage", skip_serializing_if = "Option::is_none")]
    pub column_lineage: Option<ColumnLineageDatasetFacet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symlinks: Option<SymlinksDatasetFacet>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Output-only dataset facets (serialized under `outputFacets`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OutputDatasetFacets {
    #[serde(rename = "outputStatistics", skip_serializing_if = "Option::is_none")]
    pub output_statistics: Option<OutputStatisticsOutputDatasetFacet>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl OutputDatasetFacets {
    pub fn is_empty(&self) -> bool {
        self.output_statistics.is_none() && self.extra.is_empty()
    }
}

/// Runtime statistics about the data written to an output dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputStatisticsOutputDatasetFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    #[serde(rename = "rowCount", skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(rename = "fileCount", skip_serializing_if = "Option::is_none")]
    pub file_count: Option<i64>,
}

/// Input-only dataset facets (serialized under `inputFacets`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InputDatasetFacets {
    #[serde(rename = "inputStatistics", skip_serializing_if = "Option::is_none")]
    pub input_statistics: Option<InputStatisticsInputDatasetFacet>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl InputDatasetFacets {
    pub fn is_empty(&self) -> bool {
        self.input_statistics.is_none() && self.extra.is_empty()
    }
}

/// Runtime statistics about the data read from an input dataset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputStatisticsInputDatasetFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    #[serde(rename = "rowCount", skip_serializing_if = "Option::is_none")]
    pub row_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(rename = "fileCount", skip_serializing_if = "Option::is_none")]
    pub file_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDatasetFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceDatasetFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub name: String,
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinksDatasetFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    pub identifiers: Vec<SymlinkIdentifier>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinkIdentifier {
    pub namespace: String,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
}

/// How an output dataset's fields derive from input dataset fields.
///
/// Keyed by output field name; the spec defines this facet on output datasets
/// (consumers ignore it on inputs). Fed by the positional plan resolution in
/// `crate::column`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnLineageDatasetFacet {
    #[serde(flatten)]
    pub base: BaseFacet,
    /// Output field name -> the input fields it derives from.
    pub fields: std::collections::BTreeMap<String, FieldLineage>,
    /// Dataset-level (whole-row) influences not tied to a single output column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset: Vec<InputField>,
}

/// Provenance of a single output column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldLineage {
    #[serde(rename = "inputFields")]
    pub input_fields: Vec<InputField>,
}

/// A source `(dataset, field)` an output column derives from, with how.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputField {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transformations: Vec<Transformation>,
}

/// How an input influences an output: `DIRECT` (the source value flows into the
/// output) or `INDIRECT` (influence via filter/group/join/sort). `subtype` is
/// conventionally one of `IDENTITY`, `TRANSFORMATION`, `AGGREGATION`, `FILTER`,
/// `JOIN`, `GROUP_BY`, `SORT`, `WINDOW`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transformation {
    #[serde(rename = "type")]
    pub type_: TransformationType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtype: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default)]
    pub masking: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TransformationType {
    Direct,
    Indirect,
}
