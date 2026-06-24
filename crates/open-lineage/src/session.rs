//! One-call helper to add OpenLineage instrumentation to a `SessionState`.

use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;

use crate::client::OpenLineageClient;
use crate::config::OpenLineageConfig;
use crate::context::{LineageContextProvider, StaticContextProvider};
use crate::rule::OpenLineageQueryPlanner;

/// Instrument `state` so OpenLineage events are emitted around each query.
///
/// Installs an [`OpenLineageQueryPlanner`] that emits START at plan time and a
/// [`crate::rule::LineageExtensionPlanner`] that lowers the plan-carried marker
/// into a terminal node emitting COMPLETE/FAIL at end of execution (see the
/// [`crate::rule`] module). Mirrors `datafusion_tracing::instrument_session_state`.
///
/// Use [`StaticContextProvider::default`] for the `context` argument when there is
/// no orchestration context to inject.
///
/// Note: this sets the session's query planner. A pre-existing custom
/// `QueryPlanner` is replaced; standard physical planning (including extension
/// nodes) is preserved because the new planner delegates to a
/// `DefaultPhysicalPlanner`.
pub fn instrument_session_state(
    state: SessionState,
    client: OpenLineageClient,
    context: Arc<dyn LineageContextProvider>,
    config: OpenLineageConfig,
) -> SessionState {
    let planner = Arc::new(OpenLineageQueryPlanner::new(
        client,
        context,
        config,
        Vec::new(),
    ));
    SessionStateBuilder::from(state)
        .with_query_planner(planner)
        .build()
}

/// Convenience wrapper using a [`StaticContextProvider`] with default (empty)
/// context.
pub fn instrument_session_state_simple(
    state: SessionState,
    client: OpenLineageClient,
    config: OpenLineageConfig,
) -> SessionState {
    instrument_session_state(
        state,
        client,
        Arc::new(StaticContextProvider::default()),
        config,
    )
}
