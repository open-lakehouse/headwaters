//! CLI errors and their process exit codes.

/// A CLI-level failure. Maps to a stable exit code via [`CliError::exit_code`].
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// A request to the server failed (transport error or non-OK status).
    #[error(transparent)]
    Client(#[from] headwaters_client::Error),

    /// `<TARGET>` was not a recognized nodeId or `kind:ns/name` shorthand.
    #[error(
        "not a valid target: {0}\n  expected a nodeId (job:<ns>:<name>, dataset:<ns>:<name>) \
         or shorthand (dataset:<ns>/<name>)"
    )]
    BadTarget(String),

    /// Writing output failed.
    #[error("output error: {0}")]
    Io(#[from] std::io::Error),
}

impl CliError {
    /// The process exit code for this error:
    /// `1` server/transport, `3` not-found, `2` usage (`BadTarget`), `1` I/O.
    pub fn exit_code(&self) -> u8 {
        match self {
            CliError::Client(e) if e.is_not_found() => 3,
            CliError::Client(_) => 1,
            CliError::BadTarget(_) => 2,
            CliError::Io(_) => 1,
        }
    }

    /// The short kind tag used in structured (`json`/`agent`) error output.
    pub fn kind(&self) -> &'static str {
        match self {
            CliError::Client(e) if e.is_not_found() => "not_found",
            CliError::Client(_) => "server_error",
            CliError::BadTarget(_) => "usage_error",
            CliError::Io(_) => "io_error",
        }
    }
}
