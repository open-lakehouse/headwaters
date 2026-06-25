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

# seed a running lineage-service with the rich demo lineage in examples/seed
# (regenerates the Headwaters demo graph, then POSTs it + the vendored Marquez
# food-delivery dataset to the batch endpoint). Target a non-default host with
# MARQUEZ_URL=… (defaults to http://localhost:8091). See examples/seed/README.md.
seed *files:
    examples/seed/ingest.sh {{ files }}

# the Postgres-backed read/projection acceptance tests (needs Docker; spins up
# a postgres container per test via testcontainers). On colima/Docker Desktop
# you may need to point DOCKER_HOST at the right socket first.
postgres-it:
    cargo nextest run -p lineage-service --features postgres-it --test read_test

# the differential conformance harness: brings up the real Marquez reference
# implementation alongside our service, posts identical events to both, and
# asserts they reconstruct equivalent lineage (table + column level + facets).
# Needs Docker. On colima/Docker Desktop, point DOCKER_HOST at the right socket.
conformance-it:
    cargo nextest run -p lineage-service --features conformance-it --test conformance_test

# the live Marquez reference-backend acceptance test (needs Docker; pulls
# marquezproject/marquez + postgres via testcontainers)
marquez-it:
    cargo test -p datafusion-open-lineage --features marquez-it --test marquez_acceptance -- --ignored --nocapture

# regenerate lineage-service's protobuf message types + the read-API ConnectRPC
# facade from the lineage protos (events, facets, read DTOs, service defs). The
# buffa plugin runs remotely on the BSR (no local install); the two connect
# plugins are local binaries (`cargo install protoc-gen-connect-rust
# protoc-gen-buffa-packaging`). The generated output is committed under
# crates/lineage-service/src/{proto,connect_gen}/.
proto-gen:
    buf generate
    cargo fmt -p lineage-service

# --- lineage UI (node/ monorepo: lineage-client + lineage-ui + scaffold app) ---

# install the node workspace dependencies (lineage-client, lineage-ui, app)
ui-install:
    cd node && npm install

# regenerate the read-API ConnectRPC TypeScript client from the protos. Mirrors
# `proto-gen` for the Rust side: one proto, two language clients. Output is
# committed under node/lineage-client/src/gen/.
ui-gen:
    cd node && npm run gen:rpc

# run the scaffold UI dev server (Vite). Expects the lineage-service on :8091
# (`just lineage`); the Vite proxy forwards ConnectRPC calls to it.
ui-dev:
    cd node && npm run dev

# run Storybook for the reusable lineage-ui components (mocked, no backend).
ui-sb:
    cd node && npm run storybook

# typecheck + lint the whole node workspace (what CI gates on)
ui-check:
    cd node && npm run typecheck && npm run lint:ci
