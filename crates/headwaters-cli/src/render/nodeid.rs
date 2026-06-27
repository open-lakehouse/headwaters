//! Resolve a user-supplied `<TARGET>` into a lineage `nodeId`.
//!
//! Accepted forms:
//! 1. a full nodeId — `job:<ns>:<name>`, `dataset:<ns>:<name>`, or
//!    `datasetField:<ns>:<name>:<field>` — passed through verbatim;
//! 2. a `kind:ns/name` shorthand — `dataset:analytics/orders` — where `/`
//!    separates namespace from name. Synthesized via the client's builders.
//!
//! Disambiguation matters because namespaces can be URIs (`snowflake://analytics`),
//! which contain both `:` and `/`. The rule: after the leading `kind:`, if what
//! remains still contains a `:`, it is a full nodeId (the ns/name boundary is a
//! colon); only when there is no `:` left do we read the `/` as the shorthand
//! separator. So `dataset:snowflake://analytics:t` is a full id, while
//! `dataset:analytics/orders` is shorthand.

use headwaters_client::{dataset_node_id, job_node_id};

use crate::error::CliError;

/// Resolve a `kind:ns/name` shorthand or a full nodeId. Used by the graph
/// commands, which require an explicit kind.
pub fn resolve_shorthand(target: &str) -> Result<String, CliError> {
    let Some((kind, rest)) = target.split_once(':') else {
        return Err(CliError::BadTarget(target.to_string()));
    };
    match kind {
        // A remaining `:` means the ns/name boundary is a colon → full nodeId.
        // Otherwise a `/` is the shorthand separator.
        "job" | "dataset" if !rest.contains(':') && rest.contains('/') => {
            let (ns, name) = rest.split_once('/').expect("contains '/'");
            Ok(match kind {
                "job" => job_node_id(ns, name),
                _ => dataset_node_id(ns, name),
            })
        }
        "job" | "dataset" | "datasetField" => Ok(target.to_string()),
        _ => Err(CliError::BadTarget(target.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_node_id_passes_through() {
        assert_eq!(
            resolve_shorthand("dataset:analytics:orders").unwrap(),
            "dataset:analytics:orders"
        );
        assert_eq!(resolve_shorthand("job:etl:daily").unwrap(), "job:etl:daily");
    }

    #[test]
    fn shorthand_synthesizes_node_id() {
        assert_eq!(
            resolve_shorthand("dataset:analytics/orders").unwrap(),
            "dataset:analytics:orders"
        );
        assert_eq!(resolve_shorthand("job:etl/daily").unwrap(), "job:etl:daily");
    }

    #[test]
    fn uri_namespace_full_node_id_passes_through() {
        // A URI namespace contains `/`, but the trailing `:` before the name
        // marks it as a full nodeId — it must NOT be split on the URI's slash.
        assert_eq!(
            resolve_shorthand("dataset:snowflake://analytics:gold.customer_360").unwrap(),
            "dataset:snowflake://analytics:gold.customer_360"
        );
    }

    #[test]
    fn unknown_kind_is_rejected() {
        assert!(resolve_shorthand("table:foo:bar").is_err());
        assert!(resolve_shorthand("nope").is_err());
    }
}
