use anyhow::Context;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use headwaters::cli::{Cli, Command, MigrateArgs, ServeArgs, run_healthcheck};
use headwaters::config::Config;

fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        // Synchronous: a `reqwest::blocking` probe needs no tokio runtime, so the
        // healthcheck path stays cheap. Map the result to a process exit code so
        // Docker/Compose can gate on it.
        Command::Healthcheck(args) => std::process::exit(match run_healthcheck(&args) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("healthcheck failed: {e}");
                1
            }
        }),
        Command::Serve(args) => serve(args),
        Command::Migrate(args) => migrate(args),
    }
}

/// Initialize tracing, resolve the config (file/env, then CLI overlay), and run
/// the server. Tracing init lives here (the binary), not in `headwaters::run`, so
/// the library never fights a host that already installed a subscriber.
#[tokio::main]
async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    // `--log-level` seeds `RUST_LOG` only when it is unset, so an explicit
    // `RUST_LOG` still wins (it is the env layer in the precedence model). This
    // runs at single-threaded entry, before the runtime spins up any threads, so
    // the `set_var` is sound.
    if std::env::var_os("RUST_LOG").is_none()
        && let Some(level) = &args.log_level
    {
        // SAFETY: single-threaded program entry; no other thread can be reading
        // the environment yet.
        unsafe { std::env::set_var("RUST_LOG", level) };
    }

    init_tracing();

    let mut cfg = Config::load(args.config.as_ref()).context("invalid configuration")?;
    args.overlay(&mut cfg);
    // Re-validate: the CLI overlay may have changed host/port after load's own
    // validation, and an invalid override should fail fast at startup.
    cfg.validate().context("invalid configuration")?;

    headwaters::run(cfg).await
}

/// Resolve config and apply any pending database migrations, then exit. Like
/// `serve`, this needs a tokio runtime (sqlx is async). It does not overlay
/// host/port — `MigrateArgs` only carries the config path, since migrations
/// need only the DSN that `Config::load` resolves.
#[tokio::main]
async fn migrate(args: MigrateArgs) -> anyhow::Result<()> {
    init_tracing();
    let cfg = Config::load(args.config.as_ref()).context("invalid configuration")?;
    headwaters::migrate(cfg).await
}

/// Install the tracing subscriber from `RUST_LOG`. Lives in the binary (not the
/// library) so an embedder can install its own subscriber instead.
fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
}
