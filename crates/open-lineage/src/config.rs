//! Static configuration shared across emitted events.

/// Identifies this integration on every emitted event/facet.
#[derive(Debug, Clone)]
pub struct OpenLineageConfig {
    /// `producer` URI stamped on events and facets (the emitting code).
    pub producer: String,
    /// Default job namespace when the context provides none
    /// (from `OPENLINEAGE_NAMESPACE`, falling back to `"default"`).
    pub job_namespace: String,
    /// Engine name for the `processing_engine` run facet.
    pub engine_name: String,
    /// Engine version for the `processing_engine` run facet.
    pub engine_version: String,
    /// This crate's version, for `openlineageAdapterVersion`.
    pub adapter_version: String,
}

/// Default `producer` URI for this crate.
pub const DEFAULT_PRODUCER: &str =
    "https://github.com/open-lakehouse/trestle/datafusion-open-lineage";

impl Default for OpenLineageConfig {
    fn default() -> Self {
        Self {
            producer: DEFAULT_PRODUCER.to_string(),
            job_namespace: std::env::var("OPENLINEAGE_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            engine_name: "DataFusion".to_string(),
            // The processing engine is DataFusion, so report DataFusion's
            // version here; this crate's own version is the adapter version.
            engine_version: datafusion::DATAFUSION_VERSION.to_string(),
            adapter_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}
