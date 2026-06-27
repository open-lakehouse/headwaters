# --- local dev environment knobs (override on the command line, e.g.
# `just PG_PORT=55432 pg-up`) ---

# Docker container + volume names for the local dev Postgres.
PG_CONTAINER := "headwaters-postgres"
PG_VOLUME := "headwaters-pgdata"
# Matches the testcontainers image/creds used by the integration tests.
PG_IMAGE := "postgres:16-alpine"
PG_PORT := "5432"
PG_USER := "postgres"
PG_PASSWORD := "postgres"
PG_DB := "lineage"
# DSN the lineage-service reads from DATABASE_URL.
DATABASE_URL := "postgres://" + PG_USER + ":" + PG_PASSWORD + "@localhost:" + PG_PORT + "/" + PG_DB

# the Marquez reference web UI, pointed at our (Marquez-compatible) read API.
MARQUEZ_WEB_CONTAINER := "headwaters-marquez-web"
MARQUEZ_WEB_IMAGE := "marquezproject/marquez-web:0.50.0"
MARQUEZ_WEB_PORT := "3000"
# Port our lineage-service serves on (the read API the UI talks to).
LINEAGE_PORT := "8091"

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

# run the lineage-service on the host against an already-running Postgres. Needs
# a DSN: set DATABASE_URL (e.g. postgres://user:pass@localhost:5432/lineage) or
# `postgres.url` in a config file. Override the config path with `LINEAGE_CONFIG=…`,
# or individual fields with `LINEAGE__*` env vars (e.g. LINEAGE__PORT=9000).
# For a one-command clean start that also brings up Postgres in Docker, use
# `just dev` instead.
lineage *args:
    RUST_LOG="${RUST_LOG:-lineage_service=debug}" \
    cargo run -p lineage-service -- {{ args }}

# seed a running lineage-service with the rich demo lineage in examples/seed
# (regenerates the Headwaters demo graph, then POSTs it + the vendored Marquez
# food-delivery dataset to the batch endpoint). Target a non-default host with
# MARQUEZ_URL=… (defaults to http://localhost:8091). See examples/seed/README.md.
seed *files:
    examples/seed/ingest.sh {{ files }}

# --- local dev Postgres (Docker) ---

# Idempotent: reuses (and starts, if stopped) an existing `{{ PG_CONTAINER }}`
# container, so re-running is safe. Data persists in the named volume
# `{{ PG_VOLUME }}` across restarts; use `pg-down` to wipe it. Override
# PG_PORT/PG_DB/etc. on the command line.
#
# start a local Postgres in Docker and wait until it accepts connections
pg-up:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "$(docker ps -aq -f name='^{{ PG_CONTAINER }}$')" ]; then
        echo "→ container {{ PG_CONTAINER }} exists; (re)starting"
        docker start {{ PG_CONTAINER }} >/dev/null
    else
        echo "→ creating {{ PG_CONTAINER }} ({{ PG_IMAGE }}) on :{{ PG_PORT }}"
        docker run -d \
            --name {{ PG_CONTAINER }} \
            -e POSTGRES_USER={{ PG_USER }} \
            -e POSTGRES_PASSWORD={{ PG_PASSWORD }} \
            -e POSTGRES_DB={{ PG_DB }} \
            -p {{ PG_PORT }}:5432 \
            -v {{ PG_VOLUME }}:/var/lib/postgresql/data \
            {{ PG_IMAGE }} >/dev/null
    fi
    echo -n "→ waiting for Postgres "
    for _ in $(seq 1 30); do
        if docker exec {{ PG_CONTAINER }} pg_isready -U {{ PG_USER }} -d {{ PG_DB }} >/dev/null 2>&1; then
            echo "ready"
            echo "  DATABASE_URL={{ DATABASE_URL }}"
            exit 0
        fi
        echo -n "."
        sleep 1
    done
    echo; echo "error: Postgres did not become ready in time" >&2; exit 1

# (To keep the data, `docker stop {{ PG_CONTAINER }}` instead.)
#
# remove the local dev Postgres container AND its data volume (a clean slate)
pg-down:
    -docker rm -f {{ PG_CONTAINER }} 2>/dev/null
    -docker volume rm {{ PG_VOLUME }} 2>/dev/null
    @echo "✓ removed {{ PG_CONTAINER }} and volume {{ PG_VOLUME }}"

# open a psql shell against the local dev Postgres.
pg-shell:
    docker exec -it {{ PG_CONTAINER }} psql -U {{ PG_USER }} -d {{ PG_DB }}

# Brings up Postgres, then runs the service against it (auto-migrates on boot,
# serves on :8091). Ctrl-C stops the service; the Postgres container keeps
# running — `just dev-down` tears it down. Extra args pass through to the service
# (e.g. `just dev -- --port 9000`).
#
# clean start of the whole local environment (Postgres + lineage-service)
dev *args: pg-up
    DATABASE_URL="{{ DATABASE_URL }}" \
    RUST_LOG="${RUST_LOG:-lineage_service=debug}" \
    cargo run -p lineage-service -- {{ args }}

# Stop the lineage-service with Ctrl-C first. Also removes the Marquez UI if it
# was started.
#
# clean shutdown of the local environment (removes the Postgres + Marquez containers)
dev-down: pg-down marquez-ui-down

# --- Marquez reference web UI ---

# Our read API honors the Marquez wire contract, so the upstream Marquez web UI
# can point straight at our lineage-service — handy for cross-checking the data
# against the reference frontend. Needs a running service (`just dev`) on
# LINEAGE_PORT. The container reaches the host via host.docker.internal (mapped
# to the host gateway so it works on Linux too). Idempotent. Open
# http://localhost:{{ MARQUEZ_WEB_PORT }} once it's up.
#
# spawn the Marquez reference web UI pointed at our lineage-service
marquez-ui:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "$(docker ps -aq -f name='^{{ MARQUEZ_WEB_CONTAINER }}$')" ]; then
        echo "→ container {{ MARQUEZ_WEB_CONTAINER }} exists; (re)starting"
        docker start {{ MARQUEZ_WEB_CONTAINER }} >/dev/null
    else
        if ! curl -fsS "http://localhost:{{ LINEAGE_PORT }}/health" >/dev/null 2>&1; then
            echo "warning: no lineage-service on :{{ LINEAGE_PORT }} — start it with \`just dev\`" >&2
        fi
        echo "→ creating {{ MARQUEZ_WEB_CONTAINER }} ({{ MARQUEZ_WEB_IMAGE }}) on :{{ MARQUEZ_WEB_PORT }}"
        docker run -d \
            --name {{ MARQUEZ_WEB_CONTAINER }} \
            --add-host host.docker.internal:host-gateway \
            -e MARQUEZ_HOST=host.docker.internal \
            -e MARQUEZ_PORT={{ LINEAGE_PORT }} \
            -e WEB_PORT={{ MARQUEZ_WEB_PORT }} \
            -p {{ MARQUEZ_WEB_PORT }}:{{ MARQUEZ_WEB_PORT }} \
            {{ MARQUEZ_WEB_IMAGE }} >/dev/null
    fi
    echo "✓ Marquez UI: http://localhost:{{ MARQUEZ_WEB_PORT }}  (API → host.docker.internal:{{ LINEAGE_PORT }})"

# remove the Marquez web UI container
marquez-ui-down:
    -docker rm -f {{ MARQUEZ_WEB_CONTAINER }} 2>/dev/null
    @echo "✓ removed {{ MARQUEZ_WEB_CONTAINER }}"

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

# build the SPA into static assets (node/app/dist).
ui-build:
    cd node && npm run build --workspace @headwaters/lineage-app

# run the lineage-service serving the bundled SPA on its own port (single
# origin: API + UI), the way the Docker image does. Builds the UI, stages it at
# ./web (where the service looks — see UI_DIR in src/http.rs), then runs. Like
# `just lineage`, needs a DSN (DATABASE_URL or config).
# Open http://localhost:{{ LINEAGE_PORT }}.
lineage-ui *args: ui-build
    rm -rf web && cp -r node/app/dist web
    RUST_LOG="${RUST_LOG:-lineage_service=debug}" \
    cargo run -p lineage-service -- {{ args }}
