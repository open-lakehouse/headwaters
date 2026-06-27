//! Generated protobuf message + view types.
//!
//! Produced by `just proto-gen` (the `buf.build/anthropics/buffa` plugin over
//! `proto/**/*.proto`). Committed to source — do not hand-edit the `*.rs`
//! includes; regenerate them instead.
//!
//! Two packages are generated:
//! - [`lineage::v1`] — the OpenLineage-aligned spec event/facet model and the
//!   ingest request/response shapes (`IngestService`).
//! - [`headwaters::read::v1`] — headwaters' own (non-spec) read API DTOs and the
//!   `ReadService` request/response messages the web UI consumes.
//!
//! Only message/view/enum types are generated here. The service RPCs are defined
//! in their `service.proto` files (with `google.api.http` annotations); the
//! ConnectRPC stubs + Axum routes come from a separate `protoc-gen-connect-rust`
//! / Trestle pass.

#[allow(
    dead_code,
    non_camel_case_types,
    unused_imports,
    clippy::derivable_impls,
    clippy::doc_lazy_continuation,
    clippy::match_single_binding
)]
pub mod lineage {
    pub mod v1 {
        include!("lineage.v1.rs");
    }
}

#[allow(
    dead_code,
    non_camel_case_types,
    unused_imports,
    clippy::derivable_impls,
    clippy::doc_lazy_continuation,
    clippy::match_single_binding
)]
pub mod headwaters {
    pub mod read {
        pub mod v1 {
            include!("headwaters.read.v1.rs");
        }
    }
}
