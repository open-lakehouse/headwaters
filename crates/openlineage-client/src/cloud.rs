//! Default HTTP transport backed by [`olai_http::CloudClient`].
//!
//! `CloudClient` handles auth (bearer token, Databricks, AWS/GCP credentials,
//! or unauthenticated) so this transport works against deployed, authenticated
//! OpenLineage endpoints out of the box.

use async_trait::async_trait;
use olai_http::CloudClient;
use url::Url;

use crate::event::RunEvent;
use crate::transport::{Transport, TransportError};

/// Posts OpenLineage events to an HTTP endpoint via [`CloudClient`].
#[derive(Clone)]
pub struct CloudClientTransport {
    client: CloudClient,
    endpoint: Url,
}

impl std::fmt::Debug for CloudClientTransport {
    // `CloudClient` is not `Debug`, so don't try to print it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudClientTransport")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl CloudClientTransport {
    /// Use a pre-built [`CloudClient`] (e.g. one constructed with cloud
    /// credentials via [`CloudClient::new_aws`] / [`CloudClient::new_databricks`]).
    pub fn new(client: CloudClient, endpoint: Url) -> Self {
        Self { client, endpoint }
    }

    /// Authenticate with a static bearer token (e.g. `OPENLINEAGE_API_KEY`).
    pub fn with_token(endpoint: Url, token: impl ToString) -> Self {
        Self::new(CloudClient::new_with_token(token), endpoint)
    }

    /// No authentication.
    pub fn unauthenticated(endpoint: Url) -> Self {
        Self::new(CloudClient::new_unauthenticated(), endpoint)
    }
}

#[async_trait]
impl Transport for CloudClientTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        self.client
            .post(self.endpoint.clone())
            .json(event)
            .send()
            .await
            .map_err(|e| TransportError::Other(e.to_string()))?;
        Ok(())
    }
}
