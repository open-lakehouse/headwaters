# syntax=docker/dockerfile:1
#
# Build for the headwaters binary. The build context is the repo root: the
# build needs the whole workspace — the Cargo.lock, all crates, and the
# git-pinned delta-rs / unitycatalog-rs forks resolve from there, so no sibling
# checkouts are needed. cargo-chef caches the dependency graph as a separate
# layer so source edits don't trigger a full dependency rebuild.
ARG RUST_TAG=1.96-bookworm

FROM rust:${RUST_TAG} AS chef
# protoc is available at build time for any transitive build script that shells
# out to it (prost-build / tonic-build). The committed lineage proto types are
# pre-generated, so a stock build doesn't need it — it's cheap insurance that
# also mirrors the CI toolchain.
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
ARG NO_CHEF=false
ENV NO_CHEF=${NO_CHEF}
# Optional sparse-registry mirror (e.g. an internal crates proxy). Configured in
# the shared base so every downstream stage — cargo-chef install, prepare, cook,
# and the final build — routes crates-io fetches through it. `git` deps still go
# straight to their host, so the build host needs outbound git access regardless.
ARG CRATES_PROXY=
RUN if [ -n "${CRATES_PROXY}" ]; then \
      proxy_url="${CRATES_PROXY}"; \
      case "${proxy_url}" in \
        sparse+*) ;; \
        *) proxy_url="sparse+${proxy_url}" ;; \
      esac; \
      mkdir -p /usr/local/cargo; \
      printf '[source.crates-io]\nreplace-with = "proxy"\n\n[source.proxy]\nregistry = "%s"\n' "${proxy_url}" > /usr/local/cargo/config.toml; \
      echo "Using CRATES_PROXY for cargo registry: ${proxy_url}"; \
    fi
RUN $NO_CHEF || cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN ($NO_CHEF && touch recipe.json) || cargo chef prepare --recipe-path recipe.json

# Build the bundled single-page web UI (node/ workspace: lineage-client +
# lineage-ui + app) into static assets. The Rust binary serves these as a
# fallback when built with `--features serve-ui` (see below). Kept as its own
# stage so the (large) node toolchain never reaches the runtime image and the
# npm install layer caches independently of the Rust build.
FROM node:22-bookworm-slim AS ui
WORKDIR /ui
# Lockfile + manifests first for a cacheable `npm ci` layer.
COPY node/package.json node/package-lock.json ./
COPY node/lineage-client/package.json ./lineage-client/
COPY node/lineage-ui/package.json ./lineage-ui/
COPY node/app/package.json ./app/
# The committed package-lock.json pins each tarball's `resolved` URL to whatever
# registry it was generated against — for us that's an internal mirror
# (npm-proxy.cloud.databricks.com) that CI and other external builders can't
# reach, so `npm ci` (which fetches the exact `resolved` URLs, ignoring any
# `--registry` flag) would hang on the unreachable host and fail with `npm error
# Exit handler never called!`. Re-point the host to the target registry here;
# the `integrity` hashes are unchanged (identical tarball content on any
# mirror), so the lockfile's guarantees still hold. Defaults to the public
# registry so CI works out of the box; override NPM_REGISTRY (and
# NPM_REGISTRY_FROM, the host to replace) to build behind a different mirror.
ARG NPM_REGISTRY=https://registry.npmjs.org
ARG NPM_REGISTRY_FROM=https://npm-proxy.cloud.databricks.com
# --no-audit/--no-fund drop the post-install audit + funding network calls a
# reproducible image build has no use for (and which add a needless egress
# dependency).
RUN sed -i "s#${NPM_REGISTRY_FROM}/#${NPM_REGISTRY}/#g" package-lock.json \
    && npm ci --no-audit --no-fund
# Then the sources, and build the app (tsc -b && vite build -> app/dist).
COPY node/ ./
RUN npm run build --workspace @headwaters/lineage-app

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies only — this is the cached layer.
RUN $NO_CHEF || cargo chef cook --release --recipe-path recipe.json --bin headwaters
# Build the application.
COPY . .
RUN cargo build --release --bin headwaters

# Minimal runtime: distroless cc (glibc + openssl) for the dynamically-linked
# binary, nonroot. No shell/package manager, so the healthcheck below can't shell
# out to curl/wget — it runs the binary's own `healthcheck` subcommand instead.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
ARG EXPIRES=Never
LABEL org.opencontainers.image.title="headwaters" quay.expires-after="${EXPIRES}"
COPY --from=builder /app/target/release/headwaters /usr/local/bin/app
# The service serves the bundled SPA from `./web` relative to its working
# directory (see `UI_DIR` in src/http.rs), so run from /app and drop the bundle
# there. Absent the bundle the static routes just 404 — the API still serves.
WORKDIR /app
COPY --from=ui /ui/app/dist ./web
# Documents the listen port (default 8091; override with HEADWATERS__PORT).
EXPOSE 8091
# Self-probe: the binary GETs its own /health and exits 0/1. Exec-form (JSON
# array) is REQUIRED — distroless has no shell for the string form. The probe
# reads the same config/env the server does, so it targets the right port.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/app", "healthcheck"]
# `serve` is baked in so a CMD-less `docker run` still starts the server; a probe
# (`docker run … healthcheck`) or a migration (`docker run … migrate`) overrides
# it with the full arg vector. `serve` does NOT apply migrations and refuses to
# start against a schema that is behind — run `migrate` once before the first
# `serve` against a new database (see examples/compose/docker-compose.yml).
ENTRYPOINT ["/usr/local/bin/app", "serve"]
