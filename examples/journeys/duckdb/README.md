# DuckDB lineage journey

A black-box, end-to-end lineage journey that drives a **published, third-party**
engine integration — the [duck_lineage](https://github.com/ilum-cloud/duck_lineage)
DuckDB OpenLineage community extension — against a running `headwaters`, then
verifies headwaters reconstructed the graph. It's the DuckDB sibling of the in-crate
DataFusion demo (`crates/open-lineage/examples/e2e_pipeline`) and runs the *same*
bronze → silver → gold story, so the two graphs are directly comparable.

Where the DataFusion example exercises our own instrumentation in-process (and is
covered by `tests/e2e_pipeline.rs`), this validates the **live HTTP wire path**: a
real external engine emitting OpenLineage over the network into headwaters' ingest
endpoint. It is intentionally *not* part of Rust coverage.

## What it does

- `journey.py` — installs the community extension (`INSTALL duck_lineage FROM
  community; LOAD duck_lineage;`), points it at headwaters with `SET
  duck_lineage_url=…`, and runs the medallion pipeline. Every statement
  auto-emits OpenLineage events via the extension's optimizer hook.
- `assert_lineage.py` — polls headwaters' Marquez-compatible read API and asserts
  the six datasets, the lineage graph, and column lineage were reconstructed in the
  `duckdb` namespace.

## Running

```sh
just dev            # headwaters + Postgres on :8091 (in one shell)
just duck-journey   # create a venv, install deps, run journey.py then assert_lineage.py
```

Then explore the graph alongside the DataFusion one:

```sh
just ui-dev         # or: just marquez-ui
```

The DuckDB graph lives in the `duckdb` namespace; the DataFusion demo's lives in
`datafusion`.

### Knobs

| Env var                  | Default                               | Purpose                              |
| ------------------------ | ------------------------------------- | ------------------------------------ |
| `OPENLINEAGE_URL`        | `http://localhost:8091/api/v1/lineage`| Ingest endpoint `journey.py` posts to |
| `HEADWATERS_URL`         | `http://localhost:8091`               | Read API base `assert_lineage.py` polls |
| `DUCK_LINEAGE_NAMESPACE` | `duckdb`                              | OpenLineage namespace for jobs       |
| `DUCK_LINEAGE_DEBUG`     | unset                                 | Set to echo each emitted event JSON  |

## How duck_lineage differs from our integration (notes from the review)

Reviewing duck_lineage informed the "safe wins" we folded into
`datafusion-open-lineage`, and is worth recording:

- **Config is SQL-`SET`-driven, not env/YAML.** It reads `duck_lineage_url`,
  `duck_lineage_namespace`, `duck_lineage_api_key`, etc. via DuckDB `SET`, and does
  **not** honor the standard `OPENLINEAGE_URL` / `OPENLINEAGE_NAMESPACE` /
  `openlineage.yml` conventions (only `OPENLINEAGE_PARENT_*` for orchestrator
  linkage). Our integration honors the standard env vars
  (`OpenLineageClient::from_env` + `OpenLineageConfig::from_env`).
- **Column lineage is plan-based but coarser.** Its `columnLineage` emits a flat
  `transformationType` of `"DIRECT"` / `"INDIRECT"`, where we emit the structured
  OpenLineage 1.x transformation objects (IDENTITY / TRANSFORMATION / AGGREGATION +
  FILTER / JOIN / GROUP_BY / …).
- **It emits dataset facets we now also emit** — `dataSource` and
  `lifecycleStateChange` — which is where those additions came from.

## Next: Spark

The natural follow-up is a sibling `examples/journeys/spark/` using **PySpark + the
`io.openlineage:openlineage-spark` package** (pulled via `spark.jars.packages`),
configured with the standard `spark.openlineage.transport.url` / `OPENLINEAGE_URL`
and `spark.openlineage.namespace=spark`, running the same medallion story with the
same read-API assertions, behind a `just spark-journey` recipe. PySpark brings its
own JVM, so no manual Java setup is needed.
