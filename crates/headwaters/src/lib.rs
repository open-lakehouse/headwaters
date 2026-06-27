// Generated protobuf types + the ConnectRPC facade now live in the
// `headwaters-proto` crate (so a publishable client can share the same codegen).
// Re-export them under the paths the rest of this crate already uses:
// `crate::{headwaters, lineage}` for the messages, `crate::proto` for the
// `buffa_module=crate::proto`-style alias, and `crate::connect_gen` for the
// `ReadService` server trait. Only the `server` feature of the facade is pulled
// in here; the `lineage.v1` ingest facade is generated but not mounted (ingest
// stays a hand-written OpenLineage REST surface — see `crate::http`).
pub use headwaters_proto::{connect_gen, headwaters, lineage};

/// Internal alias mirroring the pre-extraction `crate::proto` module path the
/// read layer references (`crate::proto::headwaters::read::v1`).
pub(crate) mod proto {
    pub use headwaters_proto::headwaters;
}

pub mod config;
pub mod http;
pub mod ingest;
pub mod projection;
pub mod read;
pub mod writer;

// Shared Postgres/testcontainers scaffolding for the integration tests. Lives in
// `src/` (not `tests/common/`) so inline `#[cfg(test)]` modules — notably the
// ConnectRPC handler tests in `read::connect`, which touch crate-private proto
// types — can share one bootstrap with the rest of the suite. Gated on
// `postgres-it` (the same feature its consumers need) so the default test build
// neither compiles the Docker scaffolding nor warns about it going unused.
#[cfg(all(test, feature = "postgres-it"))]
mod test_support;
