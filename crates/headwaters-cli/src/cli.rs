//! The `clap` command tree for `hw`.

use clap::{Args, Parser, Subcommand};

use crate::render::OutputMode;

/// Inspect a Headwaters data-lineage estate — for humans and agents.
#[derive(Debug, Parser)]
#[command(name = "hw", version, about, long_about = None)]
pub struct Cli {
    #[command(flatten)]
    pub global: GlobalArgs,

    #[command(subcommand)]
    pub command: Command,
}

/// Flags available on every subcommand.
#[derive(Debug, Args)]
pub struct GlobalArgs {
    /// Output format.
    #[arg(short, long, global = true, value_enum, default_value_t = OutputMode::Table, env = "HW_OUTPUT")]
    pub output: OutputMode,

    /// Server base URL.
    #[arg(
        long,
        global = true,
        default_value = "http://localhost:8091",
        env = "HW_SERVER"
    )]
    pub server: String,

    /// Do not interpret known facets; pass the raw bags through.
    #[arg(long, global = true)]
    pub raw_facets: bool,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// List all namespaces.
    Namespaces,

    /// Inspect datasets.
    #[command(subcommand)]
    Dataset(DatasetCommand),

    /// Show the lineage graph around a node.
    Lineage(LineageArgs),

    /// Trace provenance (upstream) or consumption (downstream) of a node.
    Trace(TraceArgs),

    /// Print the agent JSON schema + a glossary of the data model (no server call).
    Schema,
}

#[derive(Debug, Subcommand)]
pub enum DatasetCommand {
    /// Get one dataset by namespace and name.
    Get {
        /// Namespace.
        namespace: String,
        /// Dataset name.
        name: String,
    },
}

#[derive(Debug, Args)]
pub struct LineageArgs {
    /// A nodeId (`dataset:<ns>:<name>`) or shorthand (`dataset:<ns>/<name>`).
    pub target: String,
    /// Maximum hops to traverse (server caps at 20).
    #[arg(long, default_value_t = 2)]
    pub depth: i32,
    /// Restrict to one direction relative to the seed.
    #[arg(long, value_enum, default_value_t = Direction::Both)]
    pub direction: Direction,
}

#[derive(Debug, Args)]
pub struct TraceArgs {
    /// A nodeId (`dataset:<ns>:<name>`) or shorthand (`dataset:<ns>/<name>`).
    pub target: String,
    /// Trace upstream (provenance) or downstream (consumption).
    #[arg(long, value_enum, default_value_t = Direction::Up)]
    pub direction: Direction,
    /// Maximum hops to traverse.
    #[arg(long, default_value_t = 5)]
    pub depth: i32,
}

/// Graph traversal direction relative to the seed node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum Direction {
    /// Upstream — what feeds the node.
    Up,
    /// Downstream — what the node feeds.
    Down,
    /// Both directions (the full returned neighborhood).
    Both,
}
