//! Facet processors. Each parses one concern out of an event into mutations;
//! they are composed by the [`ProcessorRegistry`](super::registry::ProcessorRegistry).

pub mod column_lineage;
pub mod core;
pub mod schema;
