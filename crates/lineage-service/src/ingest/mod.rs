//! OpenLineage JSON ingestion: wire-format parsing into the proto event model.

pub mod converter;

pub use converter::{
    BatchFailure, BatchOutcome, ConvertError, OwnedEvent, convert_batch, convert_event,
};
