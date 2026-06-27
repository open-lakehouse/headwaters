//! The customer-facing entry point: [`OpenLineage::builder`].
//!
//! One builder assembles the three pieces an integration needs — a
//! [`Transport`]/[`OpenLineageClient`], a [`LineageContextProvider`], and an
//! [`OpenLineageConfig`] — and installs them on a `SessionState`. The common
//! case is [`OpenLineageBuilder::from_env`] followed by
//! [`OpenLineageBuilder::instrument`].

use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;

use openlineage_client::{ClientError, LineageContext, OpenLineageClient, Transport};

use crate::config::{DataFusionConfig, OpenLineageConfig};
use crate::context::{LineageContextProvider, StaticContextProvider};
use crate::rule::OpenLineageQueryPlanner;

/// Entry point for instrumenting a DataFusion session with OpenLineage.
///
/// Construct a [`OpenLineageBuilder`] with [`OpenLineage::builder`].
#[derive(Debug)]
pub struct OpenLineage;

impl OpenLineage {
    /// Start configuring OpenLineage instrumentation.
    pub fn builder() -> OpenLineageBuilder {
        OpenLineageBuilder::default()
    }
}

/// Builds and installs OpenLineage instrumentation on a [`SessionState`].
///
/// Set a transport (or client) and, optionally, a context provider and config;
/// then call [`Self::instrument`]. [`Self::from_env`] is the one-call path that
/// derives everything from the standard OpenLineage environment.
#[derive(Default)]
pub struct OpenLineageBuilder {
    client: Option<OpenLineageClient>,
    transport: Option<Arc<dyn Transport>>,
    context: Option<Arc<dyn LineageContextProvider>>,
    config: Option<OpenLineageConfig>,
}

impl std::fmt::Debug for OpenLineageBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenLineageBuilder")
            .field("has_client", &self.client.is_some())
            .field("has_transport", &self.transport.is_some())
            .field("has_context", &self.context.is_some())
            .field("config", &self.config)
            .finish()
    }
}

impl OpenLineageBuilder {
    /// Derive client, context, and config from the standard OpenLineage
    /// environment in one call.
    ///
    /// Builds the client via [`OpenLineageClient::from_env`] (reads
    /// `OPENLINEAGE_URL` / `OPENLINEAGE_ENDPOINT` / `OPENLINEAGE_API_KEY`), the
    /// config via [`OpenLineageConfig::for_datafusion_from_env`] (reads
    /// `OPENLINEAGE_NAMESPACE` / `OPENLINEAGE_TIMEOUT_MS`), and the context via
    /// [`LineageContext::from_env`] (reads the parent-run variables). Anything
    /// set explicitly on the builder beforehand is preserved.
    ///
    /// # Errors
    /// Returns [`ClientError`] if the client cannot be built — e.g. called
    /// outside a Tokio runtime, or `OPENLINEAGE_URL` is malformed.
    pub fn from_env(mut self) -> Result<Self, ClientError> {
        let config = self
            .config
            .take()
            .unwrap_or_else(OpenLineageConfig::for_datafusion_from_env);
        if self.client.is_none() && self.transport.is_none() {
            self.client = Some(OpenLineageClient::from_env()?);
        }
        if self.context.is_none() {
            let ctx = LineageContext::from_env(&config);
            self.context = Some(Arc::new(StaticContextProvider(ctx)));
        }
        self.config = Some(config);
        Ok(self)
    }

    /// Use a pre-built [`OpenLineageClient`]. Takes precedence over
    /// [`Self::transport`].
    pub fn client(mut self, client: OpenLineageClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Drain events into `transport`. A client is constructed around it at
    /// [`Self::instrument`] time. Ignored if [`Self::client`] is also set.
    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Set the per-query context provider. Defaults to an empty
    /// [`StaticContextProvider`].
    pub fn context(mut self, context: Arc<dyn LineageContextProvider>) -> Self {
        self.context = Some(context);
        self
    }

    /// Set the static config. Defaults to [`OpenLineageConfig::for_datafusion`].
    pub fn config(mut self, config: OpenLineageConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Install the instrumentation on `state`, returning the wired
    /// `SessionState`.
    ///
    /// Sets the session's query planner to an [`OpenLineageQueryPlanner`]. A
    /// pre-existing custom `QueryPlanner` is replaced; standard physical planning
    /// (including extension nodes) is preserved because the new planner delegates
    /// to a `DefaultPhysicalPlanner`.
    ///
    /// # Panics
    /// Panics if no client or transport was set and a default client must be
    /// built outside a Tokio runtime. Prefer [`Self::from_env`] (which surfaces a
    /// [`ClientError`]) or set a client built with
    /// [`OpenLineageClient::try_new`].
    pub fn instrument(self, state: SessionState) -> SessionState {
        let client = self.client.unwrap_or_else(|| match self.transport {
            Some(transport) => OpenLineageClient::new(transport),
            None => OpenLineageClient::noop(),
        });
        let context = self
            .context
            .unwrap_or_else(|| Arc::new(StaticContextProvider::default()));
        let config = self
            .config
            .unwrap_or_else(OpenLineageConfig::for_datafusion);

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
}

/// Instrument `state` with an explicit client, context provider, and config.
///
/// A thin wrapper over [`OpenLineage::builder`] for callers that already hold
/// the three pieces. Prefer the builder ([`OpenLineage::builder`]) for new code.
pub fn instrument_session_state(
    state: SessionState,
    client: OpenLineageClient,
    context: Arc<dyn LineageContextProvider>,
    config: OpenLineageConfig,
) -> SessionState {
    OpenLineage::builder()
        .client(client)
        .context(context)
        .config(config)
        .instrument(state)
}

/// Instrument `state` with an empty [`StaticContextProvider`].
///
/// A thin wrapper over [`OpenLineage::builder`]. Prefer the builder for new code.
pub fn instrument_session_state_simple(
    state: SessionState,
    client: OpenLineageClient,
    config: OpenLineageConfig,
) -> SessionState {
    OpenLineage::builder()
        .client(client)
        .config(config)
        .instrument(state)
}
