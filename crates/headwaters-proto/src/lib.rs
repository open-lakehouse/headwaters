//! Generated protobuf types and ConnectRPC stubs for Headwaters.
//!
//! Two proto packages, produced by `just proto-gen` and committed under
//! `src/proto/` + `src/connect_gen/` (regenerate; do not hand-edit):
//!
//! - [`lineage`]`::v1` — the OpenLineage-aligned event/facet model and the
//!   ingest request/response shapes.
//! - [`headwaters`]`::read::v1` — the read API DTOs and the `ReadService`
//!   request/response messages.
//!
//! The ConnectRPC facade for the read API lives in [`connect_gen`]: it carries
//! both the server-side `ReadService` trait (used by the `headwaters` service,
//! behind the `server` feature) and a `ReadServiceClient` (used by
//! `headwaters-client`, behind the `client` feature). The generated message
//! types are referenced from there via the `crate::proto` path, so this crate's
//! root must keep a `proto` module — see `buf.gen.yaml`'s `buffa_module=crate::proto`.

// The generated message/view types. Kept as `crate::proto` (not re-exported as the
// crate root) because the connect_gen code references it by that absolute path.
mod proto;
pub use proto::{headwaters, lineage};

// The generated ConnectRPC facade (read + ingest services). Public so
// `headwaters-client` can name `ReadServiceClient` and the `headwaters` server
// can name the `ReadService` trait. Carries its own `#![allow(...)]` lints.
#[path = "connect_gen/mod.rs"]
pub mod connect_gen;
