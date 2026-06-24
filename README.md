# open-lineage-rs

Rust building blocks for [OpenLineage](https://openlineage.io) on the open
lakehouse stack: emit data lineage from [Apache DataFusion](https://datafusion.apache.org)
sessions, and ingest OpenLineage events into a lakehouse events table.

This repository is a baseline extraction of the lineage crates from
[`open-lakehouse`](https://github.com/open-lakehouse), being prepared for
independent release.

## Crates

| Crate | Package | What it does |
|---|---|---|
| [`crates/open-lineage`](crates/open-lineage) | `datafusion-open-lineage` | OpenLineage integration for DataFusion sessions — emits run events (START/COMPLETE/FAIL) with input/output datasets and column-level lineage, extracted at planning time. |
| [`crates/lineage-service`](crates/lineage-service) | `lineage-service` | An HTTP service that ingests OpenLineage events, buffers them, and writes them to a Delta Lake events table. Also serves a [Marquez](https://marquezproject.ai)-compatible read API for visualization. |

## Build & test

This is a standard Cargo workspace ([`just`](https://just.systems) wraps the
common recipes):

```sh
just build       # cargo build --workspace --all-features
just test        # cargo nextest run --workspace --all-features
```

The always-on test suite includes an offline OpenLineage spec-conformance check
(`crates/open-lineage/tests/conformance.rs`) that validates emitted events
against vendored JSON Schemas — no external services required. Live-integration
tests are `#[ignore]`d; the Marquez reference-backend acceptance test is gated
behind the `marquez-it` feature (`just marquez-it`, needs Docker).

## Protobuf

The lineage event model is defined in `proto/lineage/v1/lineage.proto`. The
generated Rust types are committed under
`crates/lineage-service/src/proto/lineage.v1.rs` so the workspace builds without
a codegen step. Regenerate with `just proto-gen` (uses [`buf`](https://buf.build)
+ the remote `buffa` plugin).

## License

Apache-2.0. See [LICENSE](LICENSE).
