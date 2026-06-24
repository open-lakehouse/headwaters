# syntax=docker/dockerfile:1
#
# Build for the lineage-service binary. The build context is the repo root: the
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

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies only — this is the cached layer.
RUN $NO_CHEF || cargo chef cook --release --recipe-path recipe.json --bin lineage-service
# Build the application.
COPY . .
RUN cargo build --release --bin lineage-service

# Minimal runtime: distroless cc (glibc + openssl) for the dynamically-linked
# binary, nonroot. No shell/package manager — run healthchecks from compose.
FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
ARG EXPIRES=Never
LABEL org.opencontainers.image.title="lineage-service" quay.expires-after="${EXPIRES}"
COPY --from=builder /app/target/release/lineage-service /usr/local/bin/app
ENTRYPOINT ["/usr/local/bin/app"]
