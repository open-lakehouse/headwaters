//! End-to-end demo: a live, instrumented DataFusion pipeline emitting OpenLineage.
//!
//! Unlike the static JSON seed (`examples/seed/`), this runs the *real*
//! instrumentation path — it builds instrumented [`SessionContext`]s, executes
//! genuine multi-stage SQL transformations over Parquet files on disk, and lets
//! the crate extract lineage from the query plans and POST it to a running
//! `headwaters`. Run the service, run this, open the UI: the bronze →
//! silver → gold graph appears, covering schema facets, column-level lineage
//! (with TRANSFORMATION / FILTER / JOIN / AGGREGATION / GROUP_BY subtypes),
//! input statistics, run history, and parent/child run correlation.
//!
//! Datasets are named with real three-part `catalog.schema.table` identities
//! (`lake.bronze.raw_orders`, `lake.silver.orders`, `lake.gold.revenue_by_country`,
//! …): the layers are schemas under a `lake` catalog built from
//! [`MemoryCatalogProvider`]/[`MemorySchemaProvider`], not flat underscore names.
//!
//! The journey itself lives in [`journey`] (shared with the always-on
//! integration test `tests/e2e_pipeline.rs`); this binary only chooses a
//! transport and drives [`journey::run_pipeline`].
//!
//! ## Running
//!
//! ```sh
//! just dev      # Postgres + headwaters on :8091 (in one shell)
//! cargo run -p datafusion-open-lineage --example e2e_pipeline   # in another
//! just ui-dev   # then open the UI and explore the graph
//! ```
//!
//! Point it at a non-default service with `OPENLINEAGE_URL`
//! (default `http://localhost:8091/api/v1/lineage`); set `OPENLINEAGE_API_KEY`
//! to authenticate, or `OPENLINEAGE_URL=console` for a service-free dry run that
//! logs events via `tracing`. Parquet is written under a temp dir, logged at
//! startup.
//!
//! Re-running appends new runs to each stage's job history without duplicating
//! datasets — handy for exercising the run-history views.
//!
//! ## How lineage is captured
//!
//! Each lineage-bearing query is an `INSERT INTO <out> SELECT … FROM <ins>`: the
//! SELECT's scans of registered Parquet become input datasets (with schema +
//! input statistics) and the insert target becomes the output dataset (with
//! schema + column lineage). `INSERT INTO` is used rather than `CREATE TABLE AS
//! SELECT` because DataFusion executes a CTAS write itself, so its target never
//! reaches the instrumented planner and no output edge is captured; an insert of
//! an existing table lowers to a `Dml` node the planner sees. The output is then
//! persisted to Parquet (via an uninstrumented context, so the read emits
//! nothing) for the next stage to pick up.

use std::sync::Arc;

use datafusion_open_lineage::transport::Transport;
use datafusion_open_lineage::{CloudClientTransport, ConsoleTransport, OpenLineageClient};
use url::Url;

mod journey;

/// Build the emit transport from the environment. Defaults to the local
/// unauthenticated service; `OPENLINEAGE_URL=console` swaps in a
/// [`ConsoleTransport`] that logs each event as JSON via `tracing` — a
/// service-free dry run for eyeballing the emitted events.
fn transport_from_env() -> Arc<dyn Transport> {
    let raw = std::env::var("OPENLINEAGE_URL")
        .unwrap_or_else(|_| "http://localhost:8091/api/v1/lineage".to_string());
    if raw.eq_ignore_ascii_case("console") {
        eprintln!("→ transport: console (events logged, not sent)");
        return Arc::new(ConsoleTransport);
    }
    let url = Url::parse(&raw).expect("OPENLINEAGE_URL must be a valid URL (or `console`)");
    eprintln!("→ transport: POST {url}");
    match std::env::var("OPENLINEAGE_API_KEY") {
        Ok(token) if !token.is_empty() => Arc::new(CloudClientTransport::with_token(url, token)),
        _ => Arc::new(CloudClientTransport::unauthenticated(url)),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // So the `console` transport's `tracing` events are visible; defaults to
    // showing them, overridable via `RUST_LOG`.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "openlineage=info".into()),
        )
        .init();

    let client = OpenLineageClient::new(transport_from_env());

    // A scratch lake root for this run. Logged so you can inspect the Parquet.
    let root = std::env::temp_dir().join("headwaters-e2e");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;
    eprintln!("→ lake root: {}", root.display());

    journey::run_pipeline(&client, &root).await?;

    // Drain the queue before exit so nothing is lost. `shutdown` awaits the
    // background drain task — no post-hoc sleep needed.
    client.shutdown().await;
    eprintln!("✓ done — explore the graph in the UI (job namespaces: bronze / silver / gold)");
    Ok(())
}
