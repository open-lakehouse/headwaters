use std::env;
use std::path::{Path, PathBuf};

use config::{Config as ConfigSource, Environment, File};
use serde::Deserialize;

/// Error raised while loading configuration. A missing file (when none was
/// explicitly requested) and unset variables both fall back to documented
/// defaults and are *not* errors; a malformed file, an unparsable value, or a
/// missing database URL is, so a misconfigured deployment refuses to start
/// instead of silently running on defaults.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to load configuration: {0}")]
    Source(#[from] config::ConfigError),

    #[error(
        "a database URL is required: set `postgres.url` or the DATABASE_URL env var \
         (e.g. postgres://user:pass@host:5432/lineage)"
    )]
    MissingDatabaseUrl,
}

fn default_pool_size() -> u32 {
    10
}

fn default_projection_interval_ms() -> u64 {
    500
}

/// Postgres connection + projection settings.
///
/// The DSN is read from `postgres.url` or, preferentially, the `DATABASE_URL`
/// environment variable (so the credential never needs to live in the
/// checked-in config file). [`PostgresConfig::resolve_url`] enforces that one of
/// them is present.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PostgresConfig {
    /// Connection DSN. Falls back to `DATABASE_URL` (overlaid at load time).
    pub url: Option<String>,
    /// Connection pool size.
    pub pool_size: u32,
    /// How often the projection worker polls the event log for new rows.
    pub projection_interval_ms: u64,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: None,
            pool_size: default_pool_size(),
            projection_interval_ms: default_projection_interval_ms(),
        }
    }
}

impl PostgresConfig {
    /// The resolved DSN, or [`ConfigError::MissingDatabaseUrl`] when neither
    /// `postgres.url` nor `DATABASE_URL` is set.
    pub fn resolve_url(&self) -> Result<&str, ConfigError> {
        self.url
            .as_deref()
            .filter(|u| !u.is_empty())
            .ok_or(ConfigError::MissingDatabaseUrl)
    }
}

/// Tuning for the asynchronous buffered writer that sits between HTTP
/// ingestion and the event log.
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

/// Top-level service configuration: defaults, overlaid by an optional config
/// file, overlaid by `LINEAGE__*` environment variables.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// TCP port the HTTP + ConnectRPC server listens on.
    pub port: u16,
    /// Postgres connection + projection settings.
    pub postgres: PostgresConfig,
    /// Buffered-writer tuning for the ingest path.
    pub writer: WriterConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: default_port(),
            postgres: PostgresConfig::default(),
            writer: WriterConfig::default(),
        }
    }
}

/// Environment variable holding the path to the config file. Also accepted as
/// the binary's first positional argument (see `main`).
pub const CONFIG_PATH_ENV: &str = "HEADWATERS_CONFIG";

/// Prefix and separator for environment overrides of structured config keys,
/// e.g. `HEADWATERS__PORT=9000` or `HEADWATERS__WRITER__BUFFER_SIZE=200`.
const ENV_PREFIX: &str = "HEADWATERS";
const ENV_SEPARATOR: &str = "__";

impl Config {
    /// Load configuration by layering, lowest precedence first:
    ///
    /// 1. struct defaults,
    /// 2. the config file (TOML/YAML/… — `path` if given, otherwise the
    ///    `HEADWATERS_CONFIG` path if set; a missing file is only an error when
    ///    the path was explicitly requested),
    /// 3. `HEADWATERS__*` environment overrides (e.g. `HEADWATERS__PORT=9000`).
    ///
    /// The Postgres DSN is then overlaid from `DATABASE_URL` if set, so the
    /// credential never needs to live in the checked-in file.
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
                .try_parsing(true),
        );

        let mut cfg: Config = builder.build()?.try_deserialize()?;

        // `DATABASE_URL` is the conventional place for the DSN; let it win over
        // an absent config value but defer to an explicit `postgres.url`.
        if cfg.postgres.url.is_none()
            && let Ok(url) = env::var("DATABASE_URL")
        {
            cfg.postgres.url = Some(url);
        }

        cfg.validate()?;
        Ok(cfg)
    }

    /// Validate cross-cutting invariants that serde can't express on its own.
    fn validate(&self) -> Result<(), ConfigError> {
        // A DSN must be resolvable so a misconfigured deployment fails at
        // startup rather than on the first write.
        self.postgres.resolve_url()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(cfg.postgres.pool_size, 10);
        assert_eq!(cfg.postgres.projection_interval_ms, 500);
        assert!(cfg.postgres.url.is_none());
    }

    #[test]
    fn test_empty_file_is_all_defaults() {
        let cfg = from_toml("").unwrap();
        assert_eq!(cfg.port, 8091);
        assert_eq!(cfg.writer.buffer_size, 100);
    }

    #[test]
    fn test_partial_file_overrides_only_named_fields() {
        let cfg = from_toml(
            r#"
            port = 9000
            [postgres]
            url = "postgres://u:p@db/lineage"
            pool_size = 25
            "#,
        )
        .unwrap();
        assert_eq!(cfg.port, 9000);
        assert_eq!(
            cfg.postgres.url.as_deref(),
            Some("postgres://u:p@db/lineage")
        );
        assert_eq!(cfg.postgres.pool_size, 25);
        // Untouched fields keep their defaults.
        assert_eq!(cfg.postgres.projection_interval_ms, 500);
        assert_eq!(cfg.writer.flush_interval_ms, 500);
    }

    #[test]
    fn test_writer_settings_parse_from_file() {
        let cfg = from_toml(
            r#"
            [writer]
            buffer_size = 250
            "#,
        )
        .unwrap();
        assert_eq!(cfg.writer.buffer_size, 250);
    }

    #[test]
    fn test_malformed_value_is_error() {
        assert!(from_toml("port = \"not-a-port\"").is_err());
    }

    #[test]
    fn test_resolve_url_present() {
        let cfg = PostgresConfig {
            url: Some("postgres://u:p@db/lineage".into()),
            ..PostgresConfig::default()
        };
        assert_eq!(cfg.resolve_url().unwrap(), "postgres://u:p@db/lineage");
    }

    #[test]
    fn test_resolve_url_missing_errors() {
        let cfg = PostgresConfig::default();
        assert!(matches!(
            cfg.resolve_url(),
            Err(ConfigError::MissingDatabaseUrl)
        ));
    }

    #[test]
    fn test_validate_missing_url_errors() {
        let cfg = Config::default();
        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::MissingDatabaseUrl)
        ));
    }

    #[test]
    fn test_load_missing_explicit_path_is_error() {
        assert!(Config::load(Some("/nonexistent/headwaters.toml")).is_err());
    }
}
