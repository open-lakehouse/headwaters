//! Generated protobuf message + view types for the lineage event model.
//!
//! Produced by `make rust-gen` (the `buf.build/anthropics/buffa` plugin over
//! `proto/lineage/v1/lineage.proto`). Committed to source — do not hand-edit
//! `lineage.v1.rs`; regenerate it instead.
//!
//! Only message/view/enum types are generated (no ConnectRPC services): the
//! lineage-service ingests OpenLineage JSON over HTTP, and these types are the
//! in-memory model that `writer::schema` converts to Arrow.

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
