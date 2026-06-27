//! An async Rust client for the [Headwaters](https://github.com/open-lakehouse/headwaters)
//! read API.
//!
//! [`HeadwatersClient`] wraps the generated ConnectRPC client with one method
//! per read RPC, returning owned response messages. Build one from a base URL:
//!
//! ```no_run
//! # async fn run() -> Result<(), headwaters_client::Error> {
//! use headwaters_client::HeadwatersClient;
//!
//! let client = HeadwatersClient::connect("http://localhost:8091")?;
//!
//! // Browse, then walk the lineage graph.
//! let dataset = client.get_dataset("analytics", "orders").await?;
//! let node = headwaters_client::dataset_node_id("analytics", "orders");
//! let graph = client.get_lineage(&node, 2).await?;
//! println!("{} reachable nodes", graph.graph.len());
//! # Ok(())
//! # }
//! ```
//!
//! The open-ended `facets` / `data` / `fields` / `events` fields on the response
//! messages are `google.protobuf.Struct`; [`struct_to_json`] turns one into a
//! [`serde_json::Value`].

mod client;
mod config;
mod error;
pub mod nodeid;
pub mod types;

pub use client::HeadwatersClient;
pub use config::{DEFAULT_TIMEOUT, ENV_TIMEOUT_MS, ENV_TOKEN, ENV_URL, HeadwatersConfig};
pub use error::Error;
pub use nodeid::{dataset_field_node_id, dataset_node_id, job_node_id};
pub use types::*;
