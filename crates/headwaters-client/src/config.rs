//! Client configuration and its environment conventions.

use std::time::Duration;

use crate::Error;

/// Environment variable holding the server base URL.
pub const ENV_URL: &str = "HEADWATERS_URL";
/// Environment variable holding a bearer token for the `authorization` header.
pub const ENV_TOKEN: &str = "HEADWATERS_TOKEN";
/// Environment variable holding the per-request timeout, in milliseconds.
pub const ENV_TIMEOUT_MS: &str = "HEADWATERS_TIMEOUT_MS";

/// Default per-request timeout when none is configured.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How a [`HeadwatersClient`](crate::HeadwatersClient) reaches the server.
///
/// Build one with [`HeadwatersConfig::new`] (then the `with_*` setters) or
/// [`HeadwatersConfig::from_env`].
#[derive(Debug, Clone)]
pub struct HeadwatersConfig {
    pub(crate) base_url: String,
    pub(crate) token: Option<String>,
    pub(crate) timeout: Duration,
}

impl HeadwatersConfig {
    /// Start from a base URL (e.g. `http://localhost:8091`), default timeout, no auth.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token: None,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Read configuration from the environment:
    /// `HEADWATERS_URL` (required), `HEADWATERS_TOKEN` and `HEADWATERS_TIMEOUT_MS`
    /// (optional). A non-numeric `HEADWATERS_TIMEOUT_MS` is ignored.
    pub fn from_env() -> Result<Self, Error> {
        let base_url = std::env::var(ENV_URL).map_err(|_| Error::MissingEnvVar(ENV_URL))?;
        let mut config = Self::new(base_url);
        if let Ok(token) = std::env::var(ENV_TOKEN) {
            config.token = Some(token);
        }
        if let Some(ms) = std::env::var(ENV_TIMEOUT_MS)
            .ok()
            .and_then(|v| v.parse().ok())
        {
            config.timeout = Duration::from_millis(ms);
        }
        Ok(config)
    }

    /// Set a bearer token, sent as `authorization: Bearer <token>`.
    #[must_use]
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Set the per-request timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}
