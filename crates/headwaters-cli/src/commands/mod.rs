//! Command handlers: each fetches from the client and returns a renderable.

mod column_lineage;
mod dataset;
mod exposure;
mod job;
mod lineage;
mod namespaces;
mod schema;
mod search;
mod tags;
mod trace;

use headwaters_client::{EntityKind, HeadwatersClient};

use crate::cli::{Command, DatasetCommand, JobCommand, SearchKind};
use crate::error::CliError;
use crate::render::{self, OutputMode, RenderCtx, nodeid};

/// Run `command` against `server`, rendering the result in `mode`.
pub async fn run(
    command: Command,
    server: &str,
    mode: OutputMode,
    ctx: RenderCtx,
) -> Result<(), CliError> {
    // `hw schema` needs no server.
    if let Command::Schema = command {
        return render::emit(&schema::Schema, mode, ctx).map_err(CliError::from);
    }

    let client = HeadwatersClient::connect(server)?;

    match command {
        Command::Schema => unreachable!("handled above"),

        Command::Namespaces => {
            let resp = client.list_namespaces().await?;
            render::emit(&namespaces::Namespaces(resp), mode, ctx)?;
        }

        Command::Dataset(DatasetCommand::List(args)) => {
            let ns = args.namespace.unwrap_or_default();
            let resp = client.list_datasets(&ns, args.limit, args.offset).await?;
            render::emit(&dataset::DatasetList(resp), mode, ctx)?;
        }

        Command::Dataset(DatasetCommand::Get { namespace, name }) => {
            let resp = client.get_dataset(&namespace, &name).await?;
            render::emit(&dataset::DatasetView(resp), mode, ctx)?;
        }

        Command::Job(JobCommand::List(args)) => {
            let ns = args.namespace.unwrap_or_default();
            let resp = client.list_jobs(&ns, args.limit, args.offset).await?;
            render::emit(&job::JobList(resp), mode, ctx)?;
        }

        Command::Job(JobCommand::Get { namespace, name }) => {
            let resp = client.get_job(&namespace, &name).await?;
            render::emit(&job::JobView(resp), mode, ctx)?;
        }

        Command::Search(args) => {
            let kind = match args.kind {
                Some(SearchKind::Job) => EntityKind::JOB,
                Some(SearchKind::Dataset) => EntityKind::DATASET,
                None => EntityKind::ENTITY_KIND_UNSPECIFIED,
            };
            let ns = args.namespace.unwrap_or_default();
            let resp = client.search(&args.query, args.limit, kind, &ns).await?;
            render::emit(&search::SearchView(resp), mode, ctx)?;
        }

        Command::ColumnLineage(args) => {
            let root = nodeid::resolve_shorthand(&args.target)?;
            let graph = column_lineage::fetch(&client, &root).await?;
            render::emit(
                &column_lineage::ColumnLineageView { root, graph },
                mode,
                ctx,
            )?;
        }

        Command::Tags => {
            let resp = client.list_tags().await?;
            render::emit(&tags::Tags(resp), mode, ctx)?;
        }

        Command::Exposure(args) => {
            let resp = client.get_tag_downstream(&args.tag).await?;
            render::emit(&exposure::Exposure(resp), mode, ctx)?;
        }

        Command::Lineage(args) => {
            let root = nodeid::resolve_shorthand(&args.target)?;
            let graph = client.get_lineage(&root, args.depth).await?;
            render::emit(
                &lineage::LineageView {
                    root,
                    direction: args.direction,
                    depth: args.depth,
                    graph,
                },
                mode,
                ctx,
            )?;
        }

        Command::Trace(args) => {
            let root = nodeid::resolve_shorthand(&args.target)?;
            let graph = client.get_lineage(&root, args.depth).await?;
            render::emit(
                &trace::TraceView {
                    root,
                    direction: args.direction,
                    graph,
                },
                mode,
                ctx,
            )?;
        }
    }
    Ok(())
}
