//! `hw` — inspect a Headwaters data-lineage estate, for humans and agents.

mod cli;
mod commands;
mod error;
mod graph;
mod render;

use clap::Parser;
use serde_json::json;

use cli::Cli;
use error::CliError;
use render::{OutputMode, RenderCtx};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let mode = cli.global.output;
    let ctx = RenderCtx {
        raw_facets: cli.global.raw_facets,
    };

    if let Err(err) = commands::run(cli.command, &cli.global.server, mode, ctx).await {
        report(&err, mode);
        std::process::exit(err.exit_code() as i32);
    }
}

/// Print an error to stderr. In `json`/`agent` modes it is a structured object
/// so an agent parsing stdout-as-data still gets a parseable failure.
fn report(err: &CliError, mode: OutputMode) {
    match mode {
        OutputMode::Json | OutputMode::Agent => {
            let body = json!({ "error": err.to_string(), "kind": err.kind() });
            eprintln!("{}", serde_json::to_string(&body).unwrap_or_default());
        }
        OutputMode::Table => eprintln!("error: {err}"),
    }
}
