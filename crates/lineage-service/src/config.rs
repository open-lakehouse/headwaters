use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};

use config::{Config as ConfigSource, Environment, File};
use serde::Deserialize;

/// Which lakehouse sink the service should fan a `WriteBatch` out to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SinkKind {
    Delta,
}

/// Error raised while loading configuration. A missing file (when none was
/// explicitly requested) and unset variables both fall back to documented
/// defaults and are *not* errors; a malformed file, an unparsable value, or an
/// unsupported sink is, so a misconfigured deployment refuses to start instead
/// of silently running on defaults.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Source(#[from] config::ConfigError),

    #[error("unknown delta.mode {0:?} (known: local, unity-external, unity-managed)")]
    UnknownDeltaMode(String),

    #[error("a unity delta mode requires `delta.endpoint` or the UNITY_CATALOG_URL env var")]
    UnityMissingEndpoint,

    #[error("a unity delta mode requires delta.catalog, delta.schema, and delta.table")]
    UnityMissingTableName,

    #[cfg(not(feature = "unity"))]
    #[error(
        "config selects a unity delta mode, but this binary was built without the `unity` feature; rebuild with `--features unity`"
    )]
    UnityNotCompiled,
}

fn default_table_path() -> String {
    "/data/events".into()
}

fn default_partition_cols() -> Vec<String> {
    vec!["event_kind".into()]
}

fn default_delta_mode() -> String {
    "local".into()
}

/// Delta sink target configuration.
///
/// A flat struct with a `mode` discriminator (not a serde-tagged enum) so every field stays
/// overridable by `LINEAGE__DELTA__*` env vars — the `config` crate's env source merges
/// key-by-key and cannot drive a tagged enum. [`DeltaConfig::resolve`] converts it into the
/// validated [`DeltaTarget`].
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DeltaConfig {
    /// `"local"` (default), `"unity-external"`, or `"unity-managed"`.
    pub mode: String,

    // --- local mode ---
    /// Events-table location for `local` mode: a bare path or object-store URI.
    pub table_path: String,
    /// Partition columns. Applied to `local` and `unity-external` writes; for `unity-managed`
    /// it is only used when auto-creating the table (appends are unpartitioned in v1).
    pub partition_cols: Vec<String>,

    // --- unity modes (external + managed) ---
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    /// Unity Catalog REST endpoint. Falls back to the `UNITY_CATALOG_URL` env var (which the
    /// loader overlays into `storage_options`).
    pub endpoint: Option<String>,
    /// AWS region hint for the UC object-store factory. Falls back to `AWS_REGION`.
    pub region: Option<String>,
    /// `unity-managed` only: create the table via the managed connector if it doesn't exist.
    /// Defaults to `true` (mirrors `local`'s auto-create). Ignored by other modes.
    pub auto_create: Option<bool>,
}

impl Default for DeltaConfig {
    fn default() -> Self {
        Self {
            mode: default_delta_mode(),
            table_path: default_table_path(),
            partition_cols: default_partition_cols(),
            catalog: None,
            schema: None,
            table: None,
            endpoint: None,
            region: None,
            auto_create: None,
        }
    }
}

/// The validated, resolved Delta sink target produced by [`DeltaConfig::resolve`].
#[derive(Debug, Clone)]
pub enum DeltaTarget {
    /// A Delta table not tracked in any catalog (local path or object-store URI).
    Local {
        table_uri: String,
        storage_options: HashMap<String, String>,
        partition_cols: Vec<String>,
    },
    /// A Unity Catalog external table: resolve location + vend creds, write via plain delta-rs.
    UnityExternal(UnityTarget),
    /// A Unity Catalog catalog-managed table: commit through the managed connector.
    UnityManaged(UnityTarget),
}

/// Shared fields for the two Unity Catalog modes.
#[derive(Debug, Clone)]
pub struct UnityTarget {
    pub endpoint: String,
    pub token: Option<String>,
    pub region: Option<String>,
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub partition_cols: Vec<String>,
    /// Only meaningful for `unity-managed` (auto-create the table). Defaults to `true`.
    pub auto_create: bool,
}

impl DeltaConfig {
    /// Convert the flat config into a validated [`DeltaTarget`]. Unity modes pull the endpoint
    /// from `delta.endpoint` or `UNITY_CATALOG_URL`, the token from `UNITY_CATALOG_TOKEN`, and
    /// the region from `delta.region` or `AWS_REGION` (the latter two via `storage_options`,
    /// where the loader overlays the env vars).
    pub fn resolve(
        &self,
        storage_options: &HashMap<String, String>,
    ) -> Result<DeltaTarget, ConfigError> {
        match self.mode.as_str() {
            "local" | "raw" => Ok(DeltaTarget::Local {
                table_uri: self.table_path.clone(),
                storage_options: storage_options.clone(),
                partition_cols: self.partition_cols.clone(),
            }),
            mode @ ("unity-external" | "unity-managed") => {
                let endpoint = self
                    .endpoint
                    .clone()
                    .or_else(|| storage_options.get("unity_catalog_url").cloned())
                    .ok_or(ConfigError::UnityMissingEndpoint)?;
                let token = storage_options.get("unity_catalog_token").cloned();
                let region = self
                    .region
                    .clone()
                    .or_else(|| storage_options.get("aws_region").cloned());
                let (catalog, schema, table) = match (&self.catalog, &self.schema, &self.table) {
                    (Some(c), Some(s), Some(t)) => (c.clone(), s.clone(), t.clone()),
                    _ => return Err(ConfigError::UnityMissingTableName),
                };
                let target = UnityTarget {
                    endpoint,
                    token,
                    region,
                    catalog,
                    schema,
                    table,
                    partition_cols: self.partition_cols.clone(),
                    auto_create: self.auto_create.unwrap_or(true),
                };
                Ok(if mode == "unity-external" {
                    DeltaTarget::UnityExternal(target)
                } else {
                    DeltaTarget::UnityManaged(target)
                })
            }
            other => Err(ConfigError::UnknownDeltaMode(other.to_string())),
        }
    }
}

/// Tuning for the asynchronous buffered writer that sits between HTTP
/// ingestion and the sinks. Defaults mirror the historical Go forwarder.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct WriterConfig {
    /// Flush once this many events are buffered.
    pub buffer_size: usize,
    /// Flush at least this often, even below `buffer_size`.
    pub flush_interval_ms: u64,
    /// Bounded ingestion channel depth; `enqueue` applies backpressure once
    /// full.
    pub channel_capacity: usize,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100,
            flush_interval_ms: 500,
            channel_capacity: 1000,
        }
    }
}

fn default_port() -> u16 {
    8091
}

fn default_sinks() -> Vec<SinkKind> {
    vec![SinkKind::Delta]
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub port: u16,
    pub sinks: Vec<SinkKind>,
    pub delta: DeltaConfig,
    /// Object-store options (region, endpoint, …) forwarded to the writer.
    /// Secrets (`AWS_*`, tokens) are layered in from the environment at load
    /// time rather than read from the config file — see [`Config::load`].
    pub storage_options: HashMap<String, String>,
    pub writer: WriterConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: default_port(),
            sinks: default_sinks(),
            delta: DeltaConfig::default(),
            storage_options: HashMap::new(),
            writer: WriterConfig::default(),
        }
    }
}

/// Secrets and object-store options sourced from the environment rather than the
/// config file, and layered into [`Config::storage_options`] at load time. Keeps
/// credentials out of the checked-in file. The map key is the lowercased env-var
/// name, matching what the Delta/object-store layer expects.
const STORAGE_OPTION_ENV_KEYS: &[&str] = &[
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_ENDPOINT_URL",
    "AWS_S3_ALLOW_UNSAFE_RENAME",
    "UNITY_CATALOG_URL",
    "UNITY_CATALOG_TOKEN",
];

/// Environment variable holding the path to the config file. Also accepted as
/// the binary's first positional argument (see `main`).
pub const CONFIG_PATH_ENV: &str = "LINEAGE_CONFIG";

/// Prefix and separator for environment overrides of structured config keys,
/// e.g. `LINEAGE__PORT=9000` or `LINEAGE__WRITER__BUFFER_SIZE=200`.
const ENV_PREFIX: &str = "LINEAGE";
const ENV_SEPARATOR: &str = "__";

impl Config {
    /// Load configuration by layering, lowest precedence first:
    ///
    /// 1. struct defaults,
    /// 2. the config file (TOML/YAML/… — `path` if given, otherwise the
    ///    `LINEAGE_CONFIG` path if set; a missing file is only an error when the
    ///    path was explicitly requested),
    /// 3. `LINEAGE__*` environment overrides (e.g. `LINEAGE__PORT=9000`).
    ///
    /// Secrets and object-store options (`AWS_*`, `UNITY_CATALOG_*`) are then
    /// overlaid into `storage_options` from the environment so they never need
    /// to live in the checked-in file.
    pub fn load(path: Option<impl AsRef<Path>>) -> Result<Self, ConfigError> {
        let path = path
            .map(|p| p.as_ref().to_path_buf())
            .or_else(|| env::var_os(CONFIG_PATH_ENV).map(PathBuf::from));

        let mut builder = ConfigSource::builder();
        if let Some(path) = path {
            // Explicitly requested -> the file must exist and parse.
            builder = builder.add_source(File::from(path).required(true));
        }
        builder = builder.add_source(
            Environment::with_prefix(ENV_PREFIX)
                .separator(ENV_SEPARATOR)
                .list_separator(",")
                .with_list_parse_key("sinks")
                .with_list_parse_key("delta.partition_cols")
                .try_parsing(true),
        );

        let mut cfg: Config = builder.build()?.try_deserialize()?;

        for key in STORAGE_OPTION_ENV_KEYS {
            if let Ok(val) = env::var(key) {
                cfg.storage_options.insert(key.to_lowercase(), val);
            }
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate cross-cutting invariants that serde can't express on its own.
    fn validate(&self) -> Result<(), ConfigError> {
        // Resolve the delta target so a bad mode / missing endpoint / missing table name fails
        // at startup rather than during the first flush.
        let target = self.delta.resolve(&self.storage_options)?;
        #[cfg(not(feature = "unity"))]
        if matches!(
            target,
            DeltaTarget::UnityExternal(_) | DeltaTarget::UnityManaged(_)
        ) {
            return Err(ConfigError::UnityNotCompiled);
        }
        let _ = target;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Deserialize a TOML body into `Config` the way `load` does (defaults +
    /// file), without touching the process environment.
    fn from_toml(body: &str) -> Result<Config, config::ConfigError> {
        ConfigSource::builder()
            .add_source(File::from_str(body, config::FileFormat::Toml))
            .build()?
            .try_deserialize()
    }

    #[test]
    fn test_defaults() {
        let cfg = Config::default();
        assert_eq!(cfg.port, 8091);
        assert_eq!(cfg.delta.table_path, "/data/events");
        assert_eq!(cfg.delta.partition_cols, vec!["event_kind"]);
        assert_eq!(cfg.sinks, vec![SinkKind::Delta]);
    }

    #[test]
    fn test_empty_file_is_all_defaults() {
        let cfg = from_toml("").unwrap();
        assert_eq!(cfg.port, 8091);
        assert_eq!(cfg.sinks, vec![SinkKind::Delta]);
        assert_eq!(cfg.delta.table_path, "/data/events");
        assert_eq!(cfg.writer.buffer_size, 100);
    }

    #[test]
    fn test_partial_file_overrides_only_named_fields() {
        let cfg = from_toml(
            r#"
            port = 9000
            [delta]
            table_path = "s3://bucket/events"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.port, 9000);
        assert_eq!(cfg.delta.table_path, "s3://bucket/events");
        // Untouched fields keep their defaults.
        assert_eq!(cfg.delta.partition_cols, vec!["event_kind"]);
        assert_eq!(cfg.writer.flush_interval_ms, 500);
        assert_eq!(cfg.sinks, vec![SinkKind::Delta]);
    }

    #[test]
    fn test_sinks_and_partition_cols_parse_from_file() {
        let cfg = from_toml(
            r#"
            sinks = ["delta"]
            [delta]
            partition_cols = ["event_kind", "event_type"]
            [writer]
            buffer_size = 250
            "#,
        )
        .unwrap();
        assert_eq!(cfg.sinks, vec![SinkKind::Delta]);
        assert_eq!(cfg.delta.partition_cols, vec!["event_kind", "event_type"]);
        assert_eq!(cfg.writer.buffer_size, 250);
    }

    #[test]
    fn test_unknown_sink_is_error() {
        let err = from_toml(r#"sinks = ["hudi"]"#).unwrap_err();
        assert!(
            err.to_string().contains("hudi") || err.to_string().contains("enum"),
            "unknown sinks fail to deserialize: {err}",
        );
    }

    #[test]
    fn test_malformed_value_is_error() {
        assert!(from_toml("port = \"not-a-port\"").is_err());
    }

    #[test]
    fn test_load_from_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("lineage-service.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        write!(f, "port = 8095\n[delta]\ntable_path = \"/tmp/ev\"\n").unwrap();
        let cfg = Config::load(Some(&path)).unwrap();
        assert_eq!(cfg.port, 8095);
        assert_eq!(cfg.delta.table_path, "/tmp/ev");
    }

    #[test]
    fn test_load_missing_explicit_path_is_error() {
        assert!(Config::load(Some("/nonexistent/lineage-service.toml")).is_err());
    }

    #[test]
    fn test_writer_defaults() {
        let w = WriterConfig::default();
        assert_eq!(w.buffer_size, 100);
        assert_eq!(w.flush_interval_ms, 500);
        assert_eq!(w.channel_capacity, 1000);
        assert_eq!(Config::default().writer.buffer_size, 100);
    }

    // --- delta target modes ---

    #[test]
    fn test_default_mode_resolves_local() {
        let cfg = DeltaConfig::default();
        let target = cfg.resolve(&HashMap::new()).unwrap();
        match target {
            DeltaTarget::Local {
                table_uri,
                partition_cols,
                ..
            } => {
                assert_eq!(table_uri, "/data/events");
                assert_eq!(partition_cols, vec!["event_kind"]);
            }
            other => panic!("expected Local, got {other:?}"),
        }
    }

    #[test]
    fn test_unity_managed_resolves_with_endpoint_and_fqn() {
        let cfg = from_toml(
            r#"
            [delta]
            mode = "unity-managed"
            catalog = "demo"
            schema = "managed_demo"
            table = "events"
            endpoint = "http://uc:8081/api/2.1/unity-catalog/"
            "#,
        )
        .unwrap();
        let target = cfg.delta.resolve(&cfg.storage_options).unwrap();
        match target {
            DeltaTarget::UnityManaged(t) => {
                assert_eq!(t.catalog, "demo");
                assert_eq!(t.schema, "managed_demo");
                assert_eq!(t.table, "events");
                assert_eq!(t.endpoint, "http://uc:8081/api/2.1/unity-catalog/");
                assert!(t.auto_create, "managed defaults to auto_create=true");
            }
            other => panic!("expected UnityManaged, got {other:?}"),
        }
    }

    #[test]
    fn test_unity_external_endpoint_falls_back_to_storage_options() {
        let cfg = from_toml(
            r#"
            [delta]
            mode = "unity-external"
            catalog = "demo"
            schema = "ext"
            table = "events"
            "#,
        )
        .unwrap();
        // Endpoint not in config -> read from storage_options (where the loader puts UNITY_CATALOG_URL).
        let mut so = HashMap::new();
        so.insert(
            "unity_catalog_url".to_string(),
            "http://uc:8081/".to_string(),
        );
        so.insert("unity_catalog_token".to_string(), "tok".to_string());
        match cfg.delta.resolve(&so).unwrap() {
            DeltaTarget::UnityExternal(t) => {
                assert_eq!(t.endpoint, "http://uc:8081/");
                assert_eq!(t.token.as_deref(), Some("tok"));
            }
            other => panic!("expected UnityExternal, got {other:?}"),
        }
    }

    #[test]
    fn test_unity_mode_missing_endpoint_errors() {
        let cfg = DeltaConfig {
            mode: "unity-managed".into(),
            catalog: Some("c".into()),
            schema: Some("s".into()),
            table: Some("t".into()),
            ..DeltaConfig::default()
        };
        assert!(matches!(
            cfg.resolve(&HashMap::new()),
            Err(ConfigError::UnityMissingEndpoint)
        ));
    }

    #[test]
    fn test_unity_mode_missing_fqn_errors() {
        let cfg = DeltaConfig {
            mode: "unity-external".into(),
            endpoint: Some("http://uc/".into()),
            ..DeltaConfig::default()
        };
        assert!(matches!(
            cfg.resolve(&HashMap::new()),
            Err(ConfigError::UnityMissingTableName)
        ));
    }

    #[test]
    fn test_unknown_mode_errors() {
        let cfg = DeltaConfig {
            mode: "hudi-managed".into(),
            ..DeltaConfig::default()
        };
        assert!(matches!(
            cfg.resolve(&HashMap::new()),
            Err(ConfigError::UnknownDeltaMode(ref m)) if m == "hudi-managed"
        ));
    }
}
