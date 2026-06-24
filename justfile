# list all commands by default
_default:
    just --list

# build the whole workspace with all features
build:
    cargo build --workspace --all-features

# run the test suite (the always-on OpenLineage conformance suite needs no
# services; live-integration tests are #[ignore]d and the Marquez acceptance
# test is gated behind the `marquez-it` feature)
test:
    cargo nextest run --workspace --all-features

# run the lineage-service on the host. Override the config path with
# `LINEAGE_CONFIG=…`, or individual fields with `LINEAGE__*` env vars. Unity
# Catalog sinks additionally need UNITY_CATALOG_URL + UNITY_CATALOG_TOKEN (and
# AWS_REGION) in the env.
lineage *args:
    RUST_LOG="${RUST_LOG:-lineage_service=debug}" \
    cargo run -p lineage-service -- {{ args }}

# the live Marquez reference-backend acceptance test (needs Docker; pulls
# marquezproject/marquez + postgres via testcontainers)
marquez-it:
    cargo test -p datafusion-open-lineage --features marquez-it --test marquez_acceptance -- --ignored --nocapture

# regenerate lineage-service's protobuf message types from the lineage proto.
# One-time: the buffa plugin runs remotely on the BSR (no local install). The
# generated output is committed under crates/lineage-service/src/proto/.
proto-gen:
    buf generate --path proto/lineage/v1/lineage.proto
    cargo fmt -p lineage-service
