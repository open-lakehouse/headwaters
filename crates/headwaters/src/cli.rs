//! CLI surface for the `headwaters` server binary.
//!
//! Two subcommands:
//!   - `serve` — run the service (see [`crate::run`]). Flags overlay the layered
//!     config (`--host`/`--port`/`--log-level`/`--config`), with CLI flags taking
//!     precedence over `HEADWATERS__*` env vars, the config file, and defaults.
//!   - `healthcheck` — probe the configured `/health` endpoint and exit 0 (healthy)
//!     or non-zero (unhealthy). This is the probe the distroless image's Docker
//!     `HEALTHCHECK` runs, since distroless has no shell/`curl` for the usual form.

use std::time::Duration;

use clap::{Args, Parser, Subcommand};

use crate::config::Config;

/// `headwaters` — OpenLineage ingest + Marquez-compatible read service.
#[derive(Debug, Parser)]
#[command(name = "headwaters", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the server.
    Serve(ServeArgs),
    /// Probe the configured `/health` endpoint; exit 0 if healthy, non-zero otherwise.
    Healthcheck(HealthcheckArgs),
}

/// Arguments for `serve`. Every flag is optional and, when present, overlays the
/// value loaded from the config file / `HEADWATERS__*` env (highest precedence).
#[derive(Debug, Default, Clone, Args)]
pub struct ServeArgs {
    /// Config file path (TOML/YAML/JSON). Also read from `HEADWATERS_CONFIG`.
    #[arg(short, long, env = "HEADWATERS_CONFIG", value_name = "PATH")]
    pub config: Option<String>,

    /// Host/interface to bind. Overrides config; default 0.0.0.0 (all interfaces).
    #[arg(long)]
    pub host: Option<String>,

    /// TCP port to listen on. Overrides config; default 8091.
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Tracing filter, applied to `RUST_LOG` only when it is unset (e.g. `info`,
    /// `headwaters=debug`). An explicit `RUST_LOG` still wins.
    #[arg(long)]
    pub log_level: Option<String>,

    /// Run API-only: do not serve the bundled web UI. Overrides config; equivalent
    /// to `ui.serve = false` / `HEADWATERS__UI__SERVE=false`. Use when embedding a
    /// custom UI built on the shipped components, or serving no UI at all.
    #[arg(long)]
    pub no_ui: bool,
}

impl ServeArgs {
    /// Overlay the provided flags onto a loaded [`Config`] (highest precedence).
    pub fn overlay(&self, cfg: &mut Config) {
        if let Some(host) = &self.host {
            cfg.host = host.clone();
        }
        if let Some(port) = self.port {
            cfg.port = port;
        }
        // A bare `--no-ui` opts out of serving the UI; absent, the config/env
        // value (default `true`) stands. The flag can only disable, never
        // re-enable, so its absence never clobbers a configured `serve = true`.
        if self.no_ui {
            cfg.ui.serve = false;
        }
    }
}

/// Arguments for `healthcheck`. Shares the config/host/port resolution inputs so
/// the probe targets the same address the server binds.
#[derive(Debug, Default, Clone, Args)]
pub struct HealthcheckArgs {
    /// Config file path used to resolve the probe target. Also `HEADWATERS_CONFIG`.
    #[arg(short, long, env = "HEADWATERS_CONFIG", value_name = "PATH")]
    pub config: Option<String>,

    /// Host to connect to (overrides config). A wildcard bind host maps to loopback.
    #[arg(long)]
    pub host: Option<String>,

    /// Port to connect to (overrides config).
    #[arg(short, long)]
    pub port: Option<u16>,

    /// Probe the full health URL directly, bypassing config load entirely. Use
    /// when no `DATABASE_URL` is available to the probe process.
    #[arg(long, value_name = "URL")]
    pub url: Option<String>,

    /// Probe timeout in seconds.
    #[arg(long, default_value_t = 3)]
    pub timeout_secs: u64,
}

impl HealthcheckArgs {
    /// The URL to GET: `--url` verbatim if given, else assembled from the loaded
    /// config (file + `HEADWATERS__*` env) with `--host`/`--port` overlaid. Note
    /// the config path requires a resolvable DSN (shared in Compose); `--url`
    /// is the escape hatch when that isn't available.
    fn target_url(&self) -> anyhow::Result<String> {
        if let Some(url) = &self.url {
            return Ok(url.clone());
        }
        let mut cfg = Config::load(self.config.as_ref())?;
        if let Some(host) = &self.host {
            cfg.host = host.clone();
        }
        if let Some(port) = self.port {
            cfg.port = port;
        }
        // Re-validate after the overlay so e.g. `--host ''` fails with a clear
        // config error rather than a cryptic connect error on a malformed URL.
        cfg.validate()?;
        Ok(cfg.health_url())
    }
}

/// Run the health probe. `Ok(())` iff the endpoint returns a 2xx with body `OK`;
/// the caller maps any `Err` to a non-zero exit.
pub fn run_healthcheck(args: &HealthcheckArgs) -> anyhow::Result<()> {
    let url = args.target_url()?;
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(args.timeout_secs))
        .build()?;
    let resp = client.get(&url).send()?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("health endpoint returned {status}");
    }
    let body = resp.text()?;
    if body.trim() != "OK" {
        anyhow::bail!("unexpected health body {body:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// The derived arg tree must be internally consistent (no conflicting flags,
    /// duplicate names, etc.).
    #[test]
    fn cli_arg_tree_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn serve_parses_with_no_flags() {
        let cli = Cli::try_parse_from(["headwaters", "serve"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert!(args.config.is_none());
                assert!(args.host.is_none());
                assert!(args.port.is_none());
                assert!(args.log_level.is_none());
                assert!(!args.no_ui);
            }
            _ => panic!("expected serve"),
        }
    }

    #[test]
    fn no_ui_flag_overlays_serve() {
        // Without the flag, the configured value (default `true`) stands.
        let cli = Cli::try_parse_from(["headwaters", "serve"]).unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        let mut cfg = Config::default();
        args.overlay(&mut cfg);
        assert!(
            cfg.ui.serve,
            "absent --no-ui leaves serve at its config value"
        );

        // `--no-ui` disables serving the bundled UI.
        let cli = Cli::try_parse_from(["headwaters", "serve", "--no-ui"]).unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert!(args.no_ui);
        let mut cfg = Config::default();
        args.overlay(&mut cfg);
        assert!(!cfg.ui.serve);
    }

    #[test]
    fn serve_flags_overlay_config() {
        let cli = Cli::try_parse_from([
            "headwaters",
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            "9000",
        ])
        .unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        let mut cfg = Config::default();
        args.overlay(&mut cfg);
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 9000);
    }

    #[test]
    fn serve_short_config_flag() {
        let cli = Cli::try_parse_from(["headwaters", "serve", "-c", "x.toml"]).unwrap();
        let Command::Serve(args) = cli.command else {
            panic!("expected serve");
        };
        assert_eq!(args.config.as_deref(), Some("x.toml"));
    }

    #[test]
    fn healthcheck_parses() {
        let cli = Cli::try_parse_from(["headwaters", "healthcheck", "--port", "9000"]).unwrap();
        let Command::Healthcheck(args) = cli.command else {
            panic!("expected healthcheck");
        };
        assert_eq!(args.port, Some(9000));
        assert_eq!(args.timeout_secs, 3);
    }

    #[test]
    fn healthcheck_url_overrides_config() {
        // With `--url` the target is verbatim and no config (hence no DSN) is loaded.
        let args = HealthcheckArgs {
            url: Some("http://example.test:1234/health".into()),
            ..HealthcheckArgs::default()
        };
        assert_eq!(
            args.target_url().unwrap(),
            "http://example.test:1234/health"
        );
    }

    #[test]
    fn no_subcommand_is_an_error() {
        // A bare `headwaters` invocation is rejected (subcommand is required).
        assert!(Cli::try_parse_from(["headwaters"]).is_err());
    }
}
