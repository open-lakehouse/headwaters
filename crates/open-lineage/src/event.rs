//! OpenLineage run-event envelope.
//!
//! See <https://openlineage.io/docs/spec/object-model> and the core
//! `OpenLineage.json` spec.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::facets::{DatasetFacets, InputDatasetFacets, JobFacets, OutputDatasetFacets, RunFacets};

/// URL of the OpenLineage run-event schema this crate emits against.
pub const RUN_EVENT_SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

/// Lifecycle stage of a run, per the OpenLineage spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum RunEventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
    Other,
}

/// A single OpenLineage run event.
///
/// All events for one query share a constant [`Run::run_id`]; `eventType`
/// distinguishes START from COMPLETE/FAIL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunEvent {
    pub event_type: RunEventType,
    /// RFC3339 timestamp.
    pub event_time: String,
    pub run: Run,
    pub job: Job,
    #[serde(default)]
    pub inputs: Vec<Dataset>,
    #[serde(default)]
    pub outputs: Vec<Dataset>,
    pub producer: String,
    #[serde(rename = "schemaURL")]
    pub schema_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Run {
    pub run_id: Uuid,
    #[serde(default)]
    pub facets: RunFacets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: JobFacets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub namespace: String,
    pub name: String,
    #[serde(default)]
    pub facets: DatasetFacets,
    /// Input-only facets (e.g. runtime statistics). Serialized as `inputFacets`
    /// and only present on input datasets.
    #[serde(
        rename = "inputFacets",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub input_facets: Option<InputDatasetFacets>,
    /// Output-only facets (e.g. runtime statistics). Serialized as
    /// `outputFacets` and only present on output datasets.
    #[serde(
        rename = "outputFacets",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub output_facets: Option<OutputDatasetFacets>,
}
