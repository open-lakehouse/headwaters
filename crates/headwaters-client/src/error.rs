//! Error type for the client.

/// An error from constructing or calling the Headwaters read API.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The configured base URL could not be parsed.
    #[error("invalid base URL: {0}")]
    InvalidBaseUrl(String),

    /// A required environment variable was not set (see [`from_env`]).
    ///
    /// [`from_env`]: crate::HeadwatersClient::from_env
    #[error("missing environment variable: {0}")]
    MissingEnvVar(&'static str),

    /// The RPC failed — a transport error, or a non-OK status from the server.
    #[error("read API request failed: {0}")]
    Rpc(#[from] connectrpc::ConnectError),
}

impl Error {
    /// The Connect status code, when this is an [`Error::Rpc`] carrying one.
    ///
    /// Lets callers branch on the failure kind — e.g. distinguish a missing
    /// entity ([`is_not_found`](Self::is_not_found)) from a transport failure.
    pub fn code(&self) -> Option<connectrpc::ErrorCode> {
        match self {
            Error::Rpc(e) => Some(e.code),
            _ => None,
        }
    }

    /// True when the failure is a `not_found` status — the requested job,
    /// dataset, or run does not exist.
    pub fn is_not_found(&self) -> bool {
        self.code() == Some(connectrpc::ErrorCode::NotFound)
    }
}
