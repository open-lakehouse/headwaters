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

    #[error(
        "invalid ui.base_path {0:?}: only letters, digits, and `-._~/` are allowed \
         (e.g. /lineage)"
    )]
    InvalidBasePath(String),

    #[error("host must not be empty: set `host` or HEADWATERS__HOST (e.g. 0.0.0.0)")]
    EmptyHost,

    #[error(
        "invalid {field}: must be greater than 0 (got {value}) — a zero interval, \
         buffer/channel size, or pool size would panic a worker or wedge the \
         connection pool at startup"
    )]
    NonPositive { field: &'static str, value: u64 },
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

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_serve_ui() -> bool {
    true
}

/// Web UI serving settings.
///
/// By default the bundled single-page app is served from the service root (`/`).
/// Set [`base_path`](UiConfig::base_path) to serve it (and every API route) under
/// a sub-path instead — the "static prefix" pattern used when Headwaters sits
/// behind a gateway at e.g. `https://platform.example.com/lineage/`. The value is
/// normalized at load time (see [`Config::normalize`]): leading slash enforced,
/// trailing slash stripped, so `lineage`, `/lineage`, and `/lineage/` all become
/// `/lineage`; empty means "serve at root", unchanged from the default.
///
/// The bundled UI is one consumer of the read API; Headwaters also ships its core
/// UI components so others can build their own front-ends against the same API.
/// Set [`serve`](UiConfig::serve) to `false` to run the service API-only — the SPA
/// routes are not mounted and the bundle on disk is ignored — for deployments that
/// embed a custom UI (or none) and want only the ingest + read surface.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct UiConfig {
    /// URL prefix the UI and all API routes are served under. Empty = root.
    pub base_path: String,
    /// Whether to serve the bundled single-page app. `true` (default) mounts the
    /// SPA routes; `false` runs API-only (ingest + read), even if a bundle is on
    /// disk. The CLI `--no-ui` flag and `HEADWATERS__UI__SERVE=false` both set
    /// this.
    pub serve: bool,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            base_path: String::new(),
            serve: default_serve_ui(),
        }
    }
}

/// Top-level service configuration: defaults, overlaid by an optional config
/// file, overlaid by `HEADWATERS__*` environment variables.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Host/interface the HTTP + ConnectRPC server binds. Default `0.0.0.0`
    /// (all interfaces); set e.g. `127.0.0.1` to bind loopback only.
    pub host: String,
    /// TCP port the HTTP + ConnectRPC server listens on.
    pub port: u16,
    /// Postgres connection + projection settings.
    pub postgres: PostgresConfig,
    /// Buffered-writer tuning for the ingest path.
    pub writer: WriterConfig,
    /// Web UI serving settings (e.g. a static URL prefix).
    pub ui: UiConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            postgres: PostgresConfig::default(),
            writer: WriterConfig::default(),
            ui: UiConfig::default(),
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

        cfg.normalize();
        cfg.validate()?;
        Ok(cfg)
    }

    /// Canonicalize values that accept loose input. Currently just the UI base
    /// path: trim surrounding whitespace, drop any trailing slashes, and ensure a
    /// single leading slash, so `lineage`, `/lineage`, and `/lineage/` all become
    /// `/lineage` and the empty string (serve at root) stays empty.
    fn normalize(&mut self) {
        self.ui.base_path = normalize_base_path(&self.ui.base_path);
    }

    /// The `host:port` the server binds. Used by `headwaters::run`.
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// The URL a local health probe should GET (see the `healthcheck`
    /// subcommand). The wildcard bind hosts `0.0.0.0` / `::` are not connectable
    /// targets, so they map to the corresponding loopback address; any other
    /// host is used verbatim. A literal IPv6 host is wrapped in brackets so the
    /// authority parses (`http://[2001:db8::1]:8091/...`, per RFC 3986). The UI
    /// base path is deliberately *not* applied: `/health` is mounted at the bind
    /// root regardless of `ui.base_path` (see `http::operational_router`), so the
    /// probe stays independent of gateway routing.
    pub fn health_url(&self) -> String {
        let host = match self.host.as_str() {
            "0.0.0.0" => "127.0.0.1".to_string(),
            "::" | "[::]" => "[::1]".to_string(),
            // A literal IPv6 address (contains `:`, not already bracketed) must be
            // bracketed in a URL authority or the host/port split is ambiguous.
            h if h.contains(':') && !h.starts_with('[') => format!("[{h}]"),
            h => h.to_string(),
        };
        format!("http://{host}:{}/health", self.port)
    }

    /// Validate cross-cutting invariants that serde can't express on its own.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // A DSN must be resolvable so a misconfigured deployment fails at
        // startup rather than on the first write.
        self.postgres.resolve_url()?;

        // An empty host would produce a `:port` bind addr that fails to parse.
        if self.host.trim().is_empty() {
            return Err(ConfigError::EmptyHost);
        }

        // Zero-valued timers/sizes are caught here so a misconfig fails fast at
        // load rather than panicking a background task later:
        // `tokio::time::interval(Duration::ZERO)` and `mpsc::channel(0)` both
        // panic, a 0-connection pool can never hand out a connection (every
        // query hangs/times out), and a 0 buffer_size would flush a one-event
        // batch on every event.
        let positive = |field: &'static str, value: u64| -> Result<(), ConfigError> {
            if value == 0 {
                Err(ConfigError::NonPositive { field, value })
            } else {
                Ok(())
            }
        };
        positive("writer.flush_interval_ms", self.writer.flush_interval_ms)?;
        positive("writer.buffer_size", self.writer.buffer_size as u64)?;
        positive(
            "writer.channel_capacity",
            self.writer.channel_capacity as u64,
        )?;
        positive(
            "postgres.projection_interval_ms",
            self.postgres.projection_interval_ms,
        )?;
        positive("postgres.pool_size", self.postgres.pool_size as u64)?;
        // The base path is interpolated verbatim into the served index.html (a
        // `<base href>` attribute and a JS string) and used to rewrite request
        // paths, so restrict it to safe URL-path characters. This rejects quotes,
        // angle brackets, whitespace, and control characters at startup — closing
        // an HTML/JS-injection vector and guaranteeing it parses as a URI path.
        let bp = &self.ui.base_path;
        if !bp.is_empty()
            && !bp
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~' | b'/'))
        {
            return Err(ConfigError::InvalidBasePath(bp.clone()));
        }
        Ok(())
    }
}

/// Canonicalize a UI base path: empty stays empty; otherwise exactly one leading
/// `/` and no trailing `/` (e.g. `lineage/` -> `/lineage`, `/` -> empty).
fn normalize_base_path(raw: &str) -> String {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("/{trimmed}")
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
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 8091);
        assert_eq!(cfg.postgres.pool_size, 10);
        assert_eq!(cfg.postgres.projection_interval_ms, 500);
        assert!(cfg.postgres.url.is_none());
    }

    #[test]
    fn test_host_parses_from_file() {
        let cfg = from_toml(r#"host = "127.0.0.1""#).unwrap();
        assert_eq!(cfg.host, "127.0.0.1");
    }

    #[test]
    fn test_validate_rejects_empty_host() {
        let mut c = Config {
            postgres: PostgresConfig {
                url: Some("postgres://u:p@db/lineage".into()),
                ..PostgresConfig::default()
            },
            ..Config::default()
        };
        c.host = "  ".into();
        assert!(matches!(c.validate(), Err(ConfigError::EmptyHost)));
    }

    #[test]
    fn test_bind_addr() {
        let c = Config {
            host: "127.0.0.1".into(),
            port: 9000,
            ..Config::default()
        };
        assert_eq!(c.bind_addr(), "127.0.0.1:9000");
    }

    #[test]
    fn test_health_url_maps_wildcard_to_loopback() {
        // 0.0.0.0 -> 127.0.0.1, :: -> [::1], a real host stays verbatim, and the
        // UI base path is folded in.
        let mut c = Config::default();
        assert_eq!(c.health_url(), "http://127.0.0.1:8091/health");

        c.host = "::".into();
        assert_eq!(c.health_url(), "http://[::1]:8091/health");

        c.host = "myhost".into();
        c.port = 9000;
        assert_eq!(c.health_url(), "http://myhost:9000/health");

        // A configured base path does NOT move the probe: `/health` lives at the
        // bind root regardless, so liveness is independent of gateway routing.
        c.host = "0.0.0.0".into();
        c.port = 8091;
        c.ui.base_path = "/lineage".into();
        assert_eq!(c.health_url(), "http://127.0.0.1:8091/health");

        // A literal (non-wildcard) IPv6 host must be bracketed, and one that is
        // already bracketed is left as-is.
        let mut c = Config {
            host: "2001:db8::1".into(),
            ..Config::default()
        };
        assert_eq!(c.health_url(), "http://[2001:db8::1]:8091/health");
        c.host = "[2001:db8::1]".into();
        assert_eq!(c.health_url(), "http://[2001:db8::1]:8091/health");
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
    fn test_validate_rejects_zero_intervals_and_pool_size() {
        let url = "postgres://u:p@db/lineage";
        let base = || {
            let mut c = Config::default();
            c.postgres.url = Some(url.into());
            c
        };
        base().validate().expect("valid defaults pass");

        let mut c = base();
        c.writer.flush_interval_ms = 0;
        assert!(matches!(
            c.validate(),
            Err(ConfigError::NonPositive {
                field: "writer.flush_interval_ms",
                ..
            })
        ));

        let mut c = base();
        c.postgres.projection_interval_ms = 0;
        assert!(matches!(
            c.validate(),
            Err(ConfigError::NonPositive {
                field: "postgres.projection_interval_ms",
                ..
            })
        ));

        let mut c = base();
        c.postgres.pool_size = 0;
        assert!(matches!(
            c.validate(),
            Err(ConfigError::NonPositive {
                field: "postgres.pool_size",
                ..
            })
        ));

        // `mpsc::channel(0)` panics — must be rejected at load too.
        let mut c = base();
        c.writer.channel_capacity = 0;
        assert!(matches!(
            c.validate(),
            Err(ConfigError::NonPositive {
                field: "writer.channel_capacity",
                ..
            })
        ));
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

    #[test]
    fn test_ui_base_path_defaults_to_empty() {
        assert_eq!(Config::default().ui.base_path, "");
        let cfg = from_toml("").unwrap();
        assert_eq!(cfg.ui.base_path, "");
    }

    #[test]
    fn test_ui_base_path_parses_from_file() {
        let cfg = from_toml(
            r#"
            [ui]
            base_path = "/lineage"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.ui.base_path, "/lineage");
    }

    #[test]
    fn test_ui_serve_defaults_to_true() {
        assert!(Config::default().ui.serve);
        let cfg = from_toml("").unwrap();
        assert!(cfg.ui.serve);
    }

    #[test]
    fn test_ui_serve_parses_from_file() {
        let cfg = from_toml(
            r#"
            [ui]
            serve = false
            "#,
        )
        .unwrap();
        assert!(!cfg.ui.serve);
    }

    #[test]
    fn test_normalize_base_path() {
        // Empty / root-only collapse to "serve at root".
        assert_eq!(normalize_base_path(""), "");
        assert_eq!(normalize_base_path("   "), "");
        assert_eq!(normalize_base_path("/"), "");
        assert_eq!(normalize_base_path("///"), "");
        // Leading slash enforced, trailing slash(es) stripped, inner kept.
        assert_eq!(normalize_base_path("lineage"), "/lineage");
        assert_eq!(normalize_base_path("/lineage"), "/lineage");
        assert_eq!(normalize_base_path("/lineage/"), "/lineage");
        assert_eq!(normalize_base_path("  /lineage/  "), "/lineage");
        assert_eq!(normalize_base_path("a/b"), "/a/b");
    }

    #[test]
    fn test_validate_accepts_safe_base_paths() {
        for bp in ["", "/lineage", "/data-lineage", "/v2/lineage", "/a_b.c~d"] {
            let cfg = Config {
                postgres: PostgresConfig {
                    url: Some("postgres://u:p@db/lineage".into()),
                    ..PostgresConfig::default()
                },
                ui: UiConfig {
                    base_path: bp.into(),
                    ..UiConfig::default()
                },
                ..Config::default()
            };
            assert!(cfg.validate().is_ok(), "should accept {bp:?}");
        }
    }

    #[test]
    fn test_validate_rejects_unsafe_base_paths() {
        // Quotes / angle brackets / whitespace would let a base path break out of
        // the injected `<base href>` attribute or JS string in index.html.
        for bp in ["/x\"><script>", "/has space", "/quote\"here", "/semi;colon"] {
            let cfg = Config {
                postgres: PostgresConfig {
                    url: Some("postgres://u:p@db/lineage".into()),
                    ..PostgresConfig::default()
                },
                ui: UiConfig {
                    base_path: bp.into(),
                    ..UiConfig::default()
                },
                ..Config::default()
            };
            assert!(
                matches!(cfg.validate(), Err(ConfigError::InvalidBasePath(_))),
                "should reject {bp:?}"
            );
        }
    }
}
