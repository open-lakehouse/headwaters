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

# run the lineage-service on the host. Needs a Postgres DSN: set DATABASE_URL
# (e.g. postgres://user:pass@localhost:5432/lineage) or `postgres.url` in a
# config file. Override the config path with `LINEAGE_CONFIG=…`, or individual
# fields with `LINEAGE__*` env vars (e.g. LINEAGE__PORT=9000).
lineage *args:
    RUST_LOG="${RUST_LOG:-lineage_service=debug}" \
    cargo run -p lineage-service -- {{ args }}

# the Postgres-backed read/projection acceptance tests (needs Docker; spins up
# a postgres container per test via testcontainers). On colima/Docker Desktop
# you may need to point DOCKER_HOST at the right socket first.
postgres-it:
    cargo nextest run -p lineage-service --features postgres-it --test read_test

# the live Marquez reference-backend acceptance test (needs Docker; pulls
# marquezproject/marquez + postgres via testcontainers)
marquez-it:
    cargo test -p datafusion-open-lineage --features marquez-it --test marquez_acceptance -- --ignored --nocapture

# regenerate lineage-service's protobuf message types from the lineage protos
# (events, facets, read DTOs, service defs). One-time: the buffa plugin runs
# remotely on the BSR (no local install). The generated output is committed
# under crates/lineage-service/src/proto/.
proto-gen:
    buf generate --path proto/lineage/v1
    cargo fmt -p lineage-service
