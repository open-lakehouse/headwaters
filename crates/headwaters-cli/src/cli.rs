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

    /// Inspect jobs.
    #[command(subcommand)]
    Job(JobCommand),

    /// Find jobs and datasets by name (resolves a fuzzy name to a nodeId).
    Search(SearchArgs),

    /// Show the lineage graph around a node.
    Lineage(LineageArgs),

    /// Trace provenance (upstream) or consumption (downstream) of a node.
    Trace(TraceArgs),

    /// Show how a dataset's columns derive from upstream columns.
    #[command(name = "column-lineage")]
    ColumnLineage(ColumnLineageArgs),

    /// List the tag catalog (the sensitivity labels in use).
    Tags,

    /// Where does a tag's data end up? Every downstream field it reaches.
    Exposure(ExposureArgs),

    /// Print the agent JSON schema + a glossary of the data model (no server call).
    Schema,
}

#[derive(Debug, Subcommand)]
pub enum DatasetCommand {
    /// List datasets, optionally scoped to one namespace.
    List(ListArgs),

    /// Get one dataset by namespace and name.
    Get {
        /// Namespace.
        namespace: String,
        /// Dataset name.
        name: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum JobCommand {
    /// List jobs, optionally scoped to one namespace.
    List(ListArgs),

    /// Get one job by namespace and name.
    Get {
        /// Namespace.
        namespace: String,
        /// Job name.
        name: String,
    },
}

/// Shared paging args for the `list` subcommands.
#[derive(Debug, Args)]
pub struct ListArgs {
    /// Scope to this namespace; omit to list across all namespaces.
    pub namespace: Option<String>,
    /// Maximum results to return (0 = server default page size).
    #[arg(long, default_value_t = 0)]
    pub limit: i32,
    /// Skip this many results (page by advancing until `total` is reached).
    #[arg(long, default_value_t = 0)]
    pub offset: i32,
}

#[derive(Debug, Args)]
pub struct SearchArgs {
    /// Case-insensitive substring matched against job and dataset names.
    pub query: String,
    /// Restrict to one entity kind.
    #[arg(long, value_enum)]
    pub kind: Option<SearchKind>,
    /// Restrict to one namespace.
    #[arg(long)]
    pub namespace: Option<String>,
    /// Maximum results to return (0 = server default).
    #[arg(long, default_value_t = 0)]
    pub limit: i32,
}

/// Which entity kind to restrict a search to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SearchKind {
    Job,
    Dataset,
}

#[derive(Debug, Args)]
pub struct ColumnLineageArgs {
    /// A `dataset:<ns>/<name>` (all fields) or `datasetField:<ns>/<name>/<field>`
    /// target (nodeId or shorthand).
    pub target: String,
}

#[derive(Debug, Args)]
pub struct ExposureArgs {
    /// The tag whose downstream reach to compute (e.g. `pii`).
    pub tag: String,
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
