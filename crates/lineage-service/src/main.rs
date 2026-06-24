use std::sync::Arc;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

use lineage_service::config::{Config, DeltaTarget, SinkKind, WriterConfig};
use lineage_service::http::{self, AppState};
use lineage_service::read::LineageStore;
use lineage_service::writer::buffered::{BufferedWriter, BufferedWriterConfig};
use lineage_service::writer::delta::DeltaWriter;
use lineage_service::writer::sink::TableSink;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Config file path: first positional arg, else the LINEAGE_CONFIG env var
    // (handled inside Config::load). With neither, run on defaults + LINEAGE__*
    // env overrides.
    let config_path = std::env::args().nth(1);
    let cfg = Config::load(config_path.as_ref()).context("invalid configuration")?;
    let sinks = build_sinks(&cfg).await?;

    let writer = BufferedWriter::spawn(sinks, writer_config(&cfg.writer));
    let store = LineageStore::from_config(&cfg)
        .await
        .context("failed to initialize the lineage read store")?;
    let app = http::router(AppState {
        writer: writer.handle(),
        store,
    });

    let addr = format!("0.0.0.0:{}", cfg.port);
    tracing::info!("lineage-service listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;

    // The server has stopped accepting requests and dropped its handler state
    // (and the writer handle inside it), so the channel can now close. Drain
    // any buffered events before exiting.
    tracing::info!("draining buffered writer");
    writer.shutdown().await;
    Ok(())
}

fn writer_config(cfg: &WriterConfig) -> BufferedWriterConfig {
    BufferedWriterConfig {
        buffer_size: cfg.buffer_size,
        flush_interval: std::time::Duration::from_millis(cfg.flush_interval_ms),
        channel_capacity: cfg.channel_capacity,
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

async fn build_sinks(cfg: &Config) -> anyhow::Result<Vec<Arc<dyn TableSink>>> {
    let mut sinks: Vec<Arc<dyn TableSink>> = Vec::with_capacity(cfg.sinks.len());
    for kind in &cfg.sinks {
        match kind {
            SinkKind::Delta => {
                sinks.push(build_delta_sink(cfg).await?);
            }
        }
    }
    Ok(sinks)
}

/// Build the Delta sink for the configured table-location mode (`local`, `unity-external`, or
/// `unity-managed`). The UC modes connect/resolve (and optionally create) at startup, so a
/// misconfigured target fails fast here rather than on the first flush.
async fn build_delta_sink(cfg: &Config) -> anyhow::Result<Arc<dyn TableSink>> {
    match cfg.delta.resolve(&cfg.storage_options)? {
        DeltaTarget::Local { table_uri, .. } => {
            tracing::info!("registering local delta sink at {table_uri}");
            Ok(Arc::new(DeltaWriter::new(cfg)))
        }
        #[cfg(feature = "unity")]
        DeltaTarget::UnityExternal(target) => {
            use lineage_service::writer::unity_external::UnityExternalSink;
            tracing::info!(
                "registering unity-external delta sink: {}.{}.{}",
                target.catalog,
                target.schema,
                target.table
            );
            Ok(Arc::new(
                UnityExternalSink::connect(target)
                    .await
                    .context("failed to initialize unity-external sink")?,
            ))
        }
        #[cfg(feature = "unity")]
        DeltaTarget::UnityManaged(target) => {
            use lineage_service::writer::unity_managed::UnityManagedSink;
            tracing::info!(
                "registering unity-managed delta sink: {}.{}.{}",
                target.catalog,
                target.schema,
                target.table
            );
            Ok(Arc::new(
                UnityManagedSink::connect(target)
                    .await
                    .context("failed to initialize unity-managed sink")?,
            ))
        }
        // Unity targets can only be produced with the `unity` feature (config validation
        // rejects them otherwise), so these arms are unreachable in a non-unity build.
        #[cfg(not(feature = "unity"))]
        DeltaTarget::UnityExternal(_) | DeltaTarget::UnityManaged(_) => unreachable!(
            "unity delta mode selected without the `unity` feature; config validation should have rejected this"
        ),
    }
}
