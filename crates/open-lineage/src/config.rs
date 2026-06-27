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

/// Default job namespace when none is configured or supplied per query.
pub const DEFAULT_NAMESPACE: &str = "default";

impl OpenLineageConfig {
    /// Build a config from the standard OpenLineage environment conventions.
    ///
    /// Reads `OPENLINEAGE_NAMESPACE` for the default job namespace (falling back
    /// to [`DEFAULT_NAMESPACE`]); the engine/adapter identity is fixed for this
    /// crate. This is the documented entry point for env-driven configuration —
    /// pair it with [`OpenLineageClient::from_env`](crate::OpenLineageClient::from_env),
    /// which reads `OPENLINEAGE_URL` / `OPENLINEAGE_ENDPOINT` / `OPENLINEAGE_API_KEY`
    /// for the transport, so an integration can wire itself up entirely from the
    /// environment the rest of the OpenLineage ecosystem already uses.
    ///
    /// [`Default`] is equivalent and reads the same environment; `from_env`
    /// exists to make the env dependency explicit and discoverable at call sites.
    pub fn from_env() -> Self {
        Self::with_namespace_env(std::env::var("OPENLINEAGE_NAMESPACE").ok())
    }

    /// [`Self::from_env`] with the `OPENLINEAGE_NAMESPACE` value injected, so the
    /// fallback logic is unit-testable without mutating process-global env. An
    /// absent or empty value falls back to [`DEFAULT_NAMESPACE`].
    fn with_namespace_env(namespace: Option<String>) -> Self {
        Self {
            job_namespace: namespace
                .filter(|ns| !ns.is_empty())
                .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
            ..Self::fixed()
        }
    }

    /// The fixed (non-environment) fields: producer, engine, and adapter
    /// identity, with the fallback namespace. Shared by [`Self::from_env`] and
    /// [`Default`].
    fn fixed() -> Self {
        Self {
            producer: DEFAULT_PRODUCER.to_string(),
            job_namespace: DEFAULT_NAMESPACE.to_string(),
            engine_name: "DataFusion".to_string(),
            // The processing engine is DataFusion, so report DataFusion's
            // version here; this crate's own version is the adapter version.
            engine_version: datafusion::DATAFUSION_VERSION.to_string(),
            adapter_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl Default for OpenLineageConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_falls_back_when_unset() {
        let cfg = OpenLineageConfig::with_namespace_env(None);
        assert_eq!(cfg.job_namespace, DEFAULT_NAMESPACE);
    }

    #[test]
    fn empty_namespace_falls_back() {
        let cfg = OpenLineageConfig::with_namespace_env(Some(String::new()));
        assert_eq!(cfg.job_namespace, DEFAULT_NAMESPACE);
    }

    #[test]
    fn namespace_is_taken_from_env() {
        let cfg = OpenLineageConfig::with_namespace_env(Some("analytics".to_string()));
        assert_eq!(cfg.job_namespace, "analytics");
    }

    #[test]
    fn fixed_identity_is_stable() {
        let cfg = OpenLineageConfig::with_namespace_env(Some("x".to_string()));
        assert_eq!(cfg.producer, DEFAULT_PRODUCER);
        assert_eq!(cfg.engine_name, "DataFusion");
        assert_eq!(cfg.adapter_version, env!("CARGO_PKG_VERSION"));
        assert!(!cfg.engine_version.is_empty());
    }
}
