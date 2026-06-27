//! Command handlers: each fetches from the client and returns a renderable.

mod dataset;
mod lineage;
mod namespaces;
mod schema;
mod trace;

use headwaters_client::HeadwatersClient;

use crate::cli::{Command, DatasetCommand};
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

        Command::Dataset(DatasetCommand::Get { namespace, name }) => {
            let resp = client.get_dataset(&namespace, &name).await?;
            render::emit(&dataset::DatasetView(resp), mode, ctx)?;
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
