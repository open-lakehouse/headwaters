//! Generated protobuf message + view types for the lineage event model.
//!
//! Produced by `just proto-gen` (the `buf.build/anthropics/buffa` plugin over
//! `proto/lineage/v1/*.proto`). Committed to source — do not hand-edit
//! `lineage.v1.rs`; regenerate it instead.
//!
//! Only message/view/enum types are generated. The `LineageService` RPCs are
//! defined in `service.proto` (with `google.api.http` annotations), but buffa
//! does not emit ConnectRPC stubs — that is a separate, later codegen step. The
//! generated types back both the hand-written REST server and (eventually) the
//! Connect service.

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
