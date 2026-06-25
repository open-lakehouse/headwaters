//! Response normalization for the upstream **Marquez web UI**.
//!
//! Our read API speaks proto3-JSON: empty `repeated` fields are *omitted* from
//! the response body (buffa emits `skip_serializing_if = is_empty_vec`). That is
//! spec-correct, and our own UI's generated client fills the defaults. The
//! upstream Marquez React frontend, however, assumes these fields are always
//! present — e.g. the dashboard does `job.tags.slice(0, 3)` and `job.tags.length`
//! with no null-guard, so a job that happens to have no tags arrives as
//! `{... no "tags" key ...}`, `job.tags` is `undefined`, and `.slice` throws —
//! taking down the whole React tree (a blank page).
//!
//! This middleware bridges that gap *only* for the read responses we serve: it
//! walks every JSON object in the body and ensures the array fields Marquez
//! dereferences unguarded (`tags`, `inputs`, `outputs`, `latestRuns`, `fields`)
//! default to `[]` on job/dataset entities. It is intentionally additive — it
//! never removes or rewrites existing values, and only touches objects carrying
//! a `type` — so it changes nothing for our own UI and only "fills in the
//! blanks" the Marquez frontend needs.
//!
//! It is applied as a `map_response` layer in [`super::http::router`]. The cost
//! is one parse + re-serialize per read response; acceptable for a read API and
//! scoped to JSON bodies only.

use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use axum::response::Response;
use http_body_util::BodyExt;
use serde_json::{Map, Value};

/// Array fields Marquez dereferences unguarded on a job/dataset/graph-node
/// entity (e.g. `job.tags.slice(...)`, `dataset.fields.map(...)`,
/// `node.outEdges.map(...)` in the lineage graph layout). Missing → `[]`.
///
/// Scoped to entities carrying a `type` (`BATCH`, `DB_TABLE`, … — jobs,
/// datasets, and lineage-graph nodes all have one); namespaces, runs, and
/// `{namespace,name}` id-pairs don't, and so are left untouched.
const ARRAY_KEYS: &[&str] = &[
    "tags",
    "inputs",
    "outputs",
    "latestRuns",
    "fields",
    "inEdges",
    "outEdges",
];

/// Recursively ensure the Marquez-expected default-present keys exist on every
/// object. Only *adds* missing keys; never overwrites a present value.
fn fill_defaults(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (_k, v) in map.iter_mut() {
                fill_defaults(v);
            }
            ensure_keys(map);
        }
        Value::Array(items) => {
            for item in items.iter_mut() {
                fill_defaults(item);
            }
        }
        _ => {}
    }
}

/// Ensure the [`ARRAY_KEYS`] exist on a single object — but only on job/dataset
/// entities (those carrying a `type`). This keeps the transform conservative: we
/// don't sprinkle empty arrays onto namespaces, runs, id-pairs, or stat buckets.
fn ensure_keys(map: &mut Map<String, Value>) {
    if !map.contains_key("type") {
        return;
    }
    for &key in ARRAY_KEYS {
        map.entry(key).or_insert_with(|| Value::Array(vec![]));
    }
}

/// Axum `map_response` layer: normalize JSON read responses for Marquez.
pub async fn normalize(response: Response) -> Response {
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/json"));
    if !is_json {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(_) => return Response::from_parts(parts, Body::empty()),
    };

    let Ok(mut json) = serde_json::from_slice::<Value>(&bytes) else {
        // Not parseable as JSON (shouldn't happen for a json content-type) —
        // pass the original bytes through unchanged.
        return Response::from_parts(parts, Body::from(bytes));
    };

    fill_defaults(&mut json);
    let out = serde_json::to_vec(&json).unwrap_or_else(|_| bytes.to_vec());

    // Body length changed; drop the stale Content-Length so axum recomputes it.
    parts.headers.remove(axum::http::header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fills_missing_tags_on_a_job() {
        let mut v = serde_json::json!({
            "name": "gold.daily_revenue",
            "namespace": "snowflake://analytics",
            "type": "BATCH"
        });
        fill_defaults(&mut v);
        assert_eq!(v["tags"], serde_json::json!([]));
        assert_eq!(v["inputs"], serde_json::json!([]));
        assert_eq!(v["outputs"], serde_json::json!([]));
        assert_eq!(v["latestRuns"], serde_json::json!([]));
        assert_eq!(v["fields"], serde_json::json!([]));
    }

    #[test]
    fn does_not_overwrite_present_values() {
        let mut v = serde_json::json!({
            "name": "j",
            "type": "BATCH",
            "tags": ["certified", "domain:customer"],
            "inputs": [{"namespace": "ns", "name": "in"}]
        });
        fill_defaults(&mut v);
        assert_eq!(
            v["tags"],
            serde_json::json!(["certified", "domain:customer"])
        );
        assert_eq!(v["inputs"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn recurses_into_arrays_and_nested_objects() {
        let mut v = serde_json::json!({
            "jobs": [
                {"name": "a", "type": "BATCH"},
                {"name": "b", "type": "BATCH", "tags": ["x"]}
            ]
        });
        fill_defaults(&mut v);
        assert_eq!(v["jobs"][0]["tags"], serde_json::json!([]));
        assert_eq!(v["jobs"][1]["tags"], serde_json::json!(["x"]));
    }

    #[test]
    fn leaves_non_type_objects_alone() {
        // A namespace ({name, createdAt}) and an id-pair ({namespace, name})
        // carry no `type`, so they must not get empty arrays sprinkled on.
        let mut ns = serde_json::json!({"name": "food_delivery", "createdAt": "t"});
        fill_defaults(&mut ns);
        assert!(ns.get("tags").is_none());
        assert!(ns.get("fields").is_none());

        let mut bucket = serde_json::json!({"period": "DAY", "count": 3});
        fill_defaults(&mut bucket);
        assert!(bucket.get("tags").is_none());
    }
}
