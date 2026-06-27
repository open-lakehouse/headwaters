// Generated protobuf message + view types (committed under src/proto/, produced
// by `just proto-gen`). Re-exported flat so `crate::lineage::v1::…` paths work.
mod proto;
pub use proto::{headwaters, lineage};

// Generated ConnectRPC service facade for the read API (committed under
// src/connect_gen/, produced by `just proto-gen`). References the buffa messages
// + views in `crate::proto` via the `buffa_module=crate::proto` codegen opt. The
// module carries its own `#![allow(...)]` lints. The `lineage.v1` ingest facade
// is generated alongside but intentionally not mounted — ingest stays a
// hand-written OpenLineage REST surface (see `crate::http`).
#[path = "connect_gen/mod.rs"]
mod connect_gen;

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
