//! Marquez `nodeId` construction + parsing.
//!
//! A `nodeId` addresses a graph node as `job:<namespace>:<name>`,
//! `dataset:<namespace>:<name>`, or `datasetField:<namespace>:<name>:<field>`.
//! These helpers are independent of the wire model (they operate on `&str`), so
//! they live apart from the proto message builders in [`super::queries`].

/// Build the Marquez `nodeId` for a job.
pub fn job_node_id(namespace: &str, name: &str) -> String {
    format!("job:{namespace}:{name}")
}

/// Build the Marquez `nodeId` for a dataset.
pub fn dataset_node_id(namespace: &str, name: &str) -> String {
    format!("dataset:{namespace}:{name}")
}

/// Build the Marquez `nodeId` for a dataset field.
pub fn dataset_field_node_id(namespace: &str, dataset: &str, field: &str) -> String {
    format!("datasetField:{namespace}:{dataset}:{field}")
}

/// The two node kinds a `nodeId` can address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Job,
    Dataset,
}

/// Parse a Marquez `nodeId` of the form `job:<namespace>:<name>` or
/// `dataset:<namespace>:<name>`.
///
/// The namespace itself is frequently a URI (`open-lineage` emits
/// `s3://bucket`-style dataset namespaces per the OpenLineage naming spec), so a
/// naive "split on the first two `:`" parse mangles `dataset:s3://bucket:wh/t1`
/// into namespace `s3`, name `//bucket:wh/t1`. We mirror Marquez's NodeId
/// parsing: when the text after the kind prefix begins with a URI scheme
/// (`[a-z][a-z0-9+.-]*://`), the namespace extends through the authority and the
/// namespace/name boundary is the next `:` *after* the authority; otherwise it's
/// the first `:`. The name may still contain further `:`.
pub fn parse_node_id(node_id: &str) -> Option<(NodeKind, String, String)> {
    let (kind, rest) = node_id.split_once(':')?;
    let kind = match kind {
        "job" => NodeKind::Job,
        "dataset" => NodeKind::Dataset,
        _ => return None,
    };
    let (namespace, name) = split_namespace_name(rest)?;
    Some((kind, namespace.to_string(), name.to_string()))
}

/// Parse the `nodeId` forms the column-lineage endpoint accepts:
/// `dataset:<ns>:<name>` (all fields) or `datasetField:<ns>:<name>:<field>`
/// (one field). Returns `(namespace, dataset, Some(field))` for the latter.
pub fn parse_column_lineage_node_id(node_id: &str) -> Option<(String, String, Option<String>)> {
    if let Some((NodeKind::Dataset, namespace, name)) = parse_node_id(node_id) {
        return Some((namespace, name, None));
    }
    let rest = node_id.strip_prefix("datasetField:")?;
    let (namespace, tail) = split_namespace_name(rest)?;
    // The dataset name may itself contain `:`; the field is the last segment.
    let (dataset, field) = tail.rsplit_once(':')?;
    Some((
        namespace.to_string(),
        dataset.to_string(),
        Some(field.to_string()),
    ))
}

/// Split the `<namespace>:<name>` tail of a nodeId, honoring URI-style
/// namespaces. Returns `None` when there is no namespace/name separator.
fn split_namespace_name(rest: &str) -> Option<(&str, &str)> {
    // Search for the namespace/name boundary `:` starting *after* any
    // `scheme://authority` prefix so URI authorities aren't split apart.
    let search_from = scheme_authority_end(rest).unwrap_or_default();
    let offset = rest[search_from..].find(':')?;
    let boundary = search_from + offset;
    Some((&rest[..boundary], &rest[boundary + 1..]))
}

/// If `s` begins with a URI scheme (`[a-z][a-z0-9+.-]*://`), return the byte
/// offset of the end of its `scheme://authority` prefix (i.e. the position of
/// the `/` or `:` that terminates the authority, or the string end). Returns
/// `None` when `s` does not start with a scheme.
fn scheme_authority_end(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    // scheme: leading letter, then letters/digits/`+`/`-`/`.`
    if bytes.is_empty() || !bytes[0].is_ascii_lowercase() {
        return None;
    }
    let mut i = 1;
    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_alphanumeric() || matches!(c, b'+' | b'-' | b'.') {
            i += 1;
        } else {
            break;
        }
    }
    // require `://` immediately after the scheme
    if !s[i..].starts_with("://") {
        return None;
    }
    // authority runs from after `://` up to the next `/` or `:` (or end).
    let auth_start = i + 3;
    let auth_len = s[auth_start..]
        .find(['/', ':'])
        .unwrap_or(s.len() - auth_start);
    Some(auth_start + auth_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_plain_namespace() {
        let (kind, ns, name) = parse_node_id("dataset:ns:name").unwrap();
        assert_eq!(kind, NodeKind::Dataset);
        assert_eq!(ns, "ns");
        assert_eq!(name, "name");
    }

    #[test]
    fn parse_job_node_id() {
        let (kind, ns, name) = parse_node_id("job:my-ns:etl.daily").unwrap();
        assert_eq!(kind, NodeKind::Job);
        assert_eq!(ns, "my-ns");
        assert_eq!(name, "etl.daily");
    }

    #[test]
    fn parse_uri_namespace() {
        // The crux of C3: the s3:// authority must stay in the namespace.
        let (kind, ns, name) = parse_node_id("dataset:s3://bucket:warehouse/t1").unwrap();
        assert_eq!(kind, NodeKind::Dataset);
        assert_eq!(ns, "s3://bucket");
        assert_eq!(name, "warehouse/t1");
    }

    #[test]
    fn parse_uri_namespace_with_slash_boundary() {
        // No `:` after the authority — the name starts right after the authority,
        // which here means the boundary is the `:` between ns and the path-name.
        let (_, ns, name) = parse_node_id("dataset:s3://open-lakehouse:warehouse/db/t").unwrap();
        assert_eq!(ns, "s3://open-lakehouse");
        assert_eq!(name, "warehouse/db/t");
    }

    #[test]
    fn parse_uri_namespace_with_port_in_name() {
        // A name containing further `:` is preserved beyond the first boundary.
        let (_, ns, name) = parse_node_id("dataset:postgres://host:db.public.t:extra").unwrap();
        assert_eq!(ns, "postgres://host");
        assert_eq!(name, "db.public.t:extra");
    }

    #[test]
    fn round_trip_uri_dataset() {
        let id = dataset_node_id("s3://bucket", "warehouse/t1");
        let (kind, ns, name) = parse_node_id(&id).unwrap();
        assert_eq!(kind, NodeKind::Dataset);
        assert_eq!(ns, "s3://bucket");
        assert_eq!(name, "warehouse/t1");
    }

    #[test]
    fn parse_rejects_unknown_kind_and_missing_separator() {
        assert!(parse_node_id("widget:ns:name").is_none());
        assert!(parse_node_id("dataset:only-namespace").is_none());
        assert!(parse_node_id("nocolon").is_none());
    }
}
