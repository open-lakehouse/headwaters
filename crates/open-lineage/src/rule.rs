//! Plan-carried lineage marker and its lowering into the terminal node.
//!
//! OpenLineage instrumentation has three concerns with different needs (see ADR
//! 0014): lineage *extraction* needs the optimized `LogicalPlan`; the START event
//! and orchestration context need `&SessionState` and are async; the terminal
//! COMPLETE/FAIL node needs to sit at the physical root and observe execution.
//! Only the [`QueryPlanner`] seam has `&SessionState`, so the planning-time work
//! lives there — but the terminal node is installed the composable, DataFusion-
//! idiomatic way: a registered [`ExtensionPlanner`] lowers a plan-carried marker
//! into [`OpenLineageExec`], rather than the planner hand-wrapping the physical
//! root.
//!
//! Flow, all under one `run_id`. First, [`OpenLineageQueryPlanner`] (the
//! [`QueryPlanner`]) extracts lineage from the optimized logical plan, resolves
//! the async [`LineageContextProvider`], emits START, builds the COMPLETE
//! template, and wraps the *logical* plan in a [`LineageMarker`] carrying that
//! template. Then physical planning lowers the marker via
//! [`LineageExtensionPlanner`] into an [`OpenLineageExec`] at the root, which
//! emits COMPLETE/FAIL at end of execution.
//!
//! A `LogicalPlan::Extension` requires a registered `ExtensionPlanner` (the
//! default physical planner errors on unknown extension nodes), so the planner
//! delegates physical planning to a [`DefaultPhysicalPlanner`] configured with
//! [`LineageExtensionPlanner`].

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{
    Expr, Extension, InvariantLevel, LogicalPlan, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use uuid::Uuid;

use crate::builder::{complete_event, fail_event, start_event};
use crate::client::OpenLineageClient;
use crate::config::OpenLineageConfig;
use crate::context::LineageContextProvider;
use crate::event::RunEvent;
use crate::exec::OpenLineageExec;
use crate::extract::extract;

// ---------------------------------------------------------------------------
// The plan-carried marker.
// ---------------------------------------------------------------------------

/// A logical no-op wrapping the real plan, carrying the per-query lineage payload
/// from [`OpenLineageQueryPlanner`] (which has `&SessionState`) to
/// [`LineageExtensionPlanner`] (which installs the terminal node). Schema-
/// transparent: it reports its input's schema so optimization and physical
/// planning treat it as a pass-through.
#[derive(Clone)]
pub struct LineageMarker {
    input: LogicalPlan,
    /// COMPLETE event template, built at plan time; cloned into the terminal
    /// [`OpenLineageExec`] at lowering and mutated into FAIL there on error.
    complete: RunEvent,
    client: OpenLineageClient,
    producer: String,
}

impl fmt::Debug for LineageMarker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LineageMarker").finish_non_exhaustive()
    }
}

// Identity is the run id plus the wrapped plan: enough to distinguish markers,
// and the payload (client/template) is behavioral rather than structural.
impl PartialEq for LineageMarker {
    fn eq(&self, other: &Self) -> bool {
        self.complete.run.run_id == other.complete.run.run_id && self.input == other.input
    }
}
impl Eq for LineageMarker {}
impl PartialOrd for LineageMarker {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.complete
            .run
            .run_id
            .partial_cmp(&other.complete.run.run_id)
    }
}
impl Hash for LineageMarker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.complete.run.run_id.hash(state);
    }
}

impl UserDefinedLogicalNodeCore for LineageMarker {
    fn name(&self) -> &str {
        "LineageMarker"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn check_invariants(&self, _check: InvariantLevel) -> Result<()> {
        Ok(())
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LineageMarker")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input: inputs.pop().expect("LineageMarker has one input"),
            complete: self.complete.clone(),
            client: self.client.clone(),
            producer: self.producer.clone(),
        })
    }
}

// ---------------------------------------------------------------------------
// Lowering: marker -> OpenLineageExec.
// ---------------------------------------------------------------------------

/// Lowers a [`LineageMarker`] into an [`OpenLineageExec`] at physical-planning
/// time. Register it on the session's physical planner (see
/// [`crate::session::instrument_session_state`]).
#[derive(Debug, Default)]
pub struct LineageExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for LineageExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Not our node: let another extension planner handle it.
        let Some(marker) = node.as_any().downcast_ref::<LineageMarker>() else {
            return Ok(None);
        };
        let inner = physical_inputs
            .first()
            .expect("LineageMarker has one physical input")
            .clone();
        Ok(Some(OpenLineageExec::new(
            inner,
            marker.client.clone(),
            marker.complete.clone(),
            marker.producer.clone(),
        )))
    }
}

// ---------------------------------------------------------------------------
// The query planner: extract + START + inject the marker.
// ---------------------------------------------------------------------------

/// A [`QueryPlanner`] that emits OpenLineage events around a query.
///
/// It does the `&SessionState`-bound, async work — extract lineage, resolve
/// context, emit START, mint the `run_id`, emit FAIL on a planning error — then
/// hands off to physical planning by wrapping the logical plan in a
/// [`LineageMarker`]. The registered [`LineageExtensionPlanner`] lowers that
/// marker into the terminal [`OpenLineageExec`]. Built by
/// [`crate::session::instrument_session_state`].
pub struct OpenLineageQueryPlanner {
    client: OpenLineageClient,
    context: Arc<dyn LineageContextProvider>,
    config: OpenLineageConfig,
    /// Physical planner that knows how to lower [`LineageMarker`]; composes any
    /// extension planners the host already had.
    physical: Arc<DefaultPhysicalPlanner>,
}

impl OpenLineageQueryPlanner {
    /// Build a planner whose physical planning lowers our marker plus
    /// `extra_extension_planners` (any the host session already registered).
    pub fn new(
        client: OpenLineageClient,
        context: Arc<dyn LineageContextProvider>,
        config: OpenLineageConfig,
        extra_extension_planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        let mut planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(LineageExtensionPlanner)];
        planners.extend(extra_extension_planners);
        Self {
            client,
            context,
            config,
            physical: Arc::new(DefaultPhysicalPlanner::with_extension_planners(planners)),
        }
    }
}

impl fmt::Debug for OpenLineageQueryPlanner {
    // `DefaultPhysicalPlanner` is not `Debug`, so don't try to print it.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenLineageQueryPlanner")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl QueryPlanner for OpenLineageQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut lineage = extract(logical_plan, &self.config);
        let cx = self.context.context(session_state).await;
        // The SQL text isn't recoverable from the plan; take it from the
        // host-supplied context (absent on non-SQL paths, e.g. ingest).
        lineage.sql = cx.sql.clone();

        // Suppress lineage for queries that touch no datasets — information_schema
        // introspection, `SET`/`SHOW`, metadata-RPC probes. They carry no input or
        // output, so a START/COMPLETE pair only adds a dangling job node to the
        // graph. Plan straight through without a marker so no events fire.
        if lineage.inputs.is_empty() && lineage.outputs.is_empty() {
            return self
                .physical
                .create_physical_plan(logical_plan, session_state)
                .await;
        }

        let run_id = cx.run_id.unwrap_or_else(Uuid::now_v7);
        self.client
            .emit(start_event(run_id, &lineage, &cx, &self.config));

        // Carry the COMPLETE template into the physical phase via the plan itself;
        // the extension planner lowers it into an OpenLineageExec at the root that
        // emits COMPLETE/FAIL at end of execution, under this same run id.
        let marker = LineageMarker {
            input: logical_plan.clone(),
            complete: complete_event(run_id, &lineage, &cx, &self.config),
            client: self.client.clone(),
            producer: self.config.producer.clone(),
        };
        let wrapped = LogicalPlan::Extension(Extension {
            node: Arc::new(marker),
        });

        match self
            .physical
            .create_physical_plan(&wrapped, session_state)
            .await
        {
            Ok(plan) => Ok(plan),
            Err(err) => {
                // Planning failed outright — no execution to observe, emit FAIL now.
                self.client.emit(fail_event(
                    run_id,
                    &lineage,
                    &cx,
                    &self.config,
                    &err.to_string(),
                ));
                Err(err)
            }
        }
    }
}
