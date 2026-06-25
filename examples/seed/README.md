# Seed data — rich demo lineage for the UI

Start a lineage-service, run one script, and get a fully-populated lineage store
to explore the UI against. The data is hand-designed to exercise **every**
read-API feature the UI surfaces, not just the happy path.

```sh
just lineage          # start the service (needs a Postgres DSN — see below)
just seed             # ingest the demo data into it
just ui-dev           # open the UI against it
```

`just seed` is a thin wrapper over [`ingest.sh`](ingest.sh).

## What's here

| File | What it is |
|---|---|
| [`generate.py`](generate.py) | Generator for the primary Headwaters demo graph. Deterministic (no wall-clock, no randomness) — re-running produces byte-identical output. |
| [`headwaters_demo.json`](headwaters_demo.json) | The generated graph, committed so you can ingest without Python. Regenerate with `python3 generate.py -o headwaters_demo.json`. |
| [`marquez_food_delivery.json`](marquez_food_delivery.json) | The canonical Marquez "food delivery" reference dataset, vendored for a second, familiar demo graph. See [provenance](#marquez-reference-data) below. |
| [`ingest.sh`](ingest.sh) | POSTs the datasets to a running service's batch endpoint and prints a per-file summary. |

## The Headwaters demo graph

A retail / e-commerce analytics platform spanning five namespaces (a streaming
source, an OLTP store, an object-store lake, a warehouse, and a BI extract):

```
kafka://events.prod        topic.clickstream
                                  │ (streaming ingest)
postgres://warehouse.prod   public.orders, public.customers
                                  │ (CDC)
s3://acme-datalake          bronze.clickstream, bronze.orders, bronze.customers ← PII
                                  │ (clean / dedup / sessionize / mask)
snowflake://analytics       silver.customers, silver.orders, silver.sessions
                                  │ (join / aggregate)
                            gold.customer_360, gold.daily_revenue
                                  │ (extract)
bigquery://reporting        marts.exec_customer_overview
```

### Features it exercises

- **Multiple namespaces / systems** — object store, OLTP, warehouse, stream, BI.
- **Many jobs** across `BATCH` and `STREAMING` integrations (Flink, Airbyte,
  Spark, dbt, Fivetran).
- **Full run lifecycle** — `START` → `COMPLETE`, plus a **`FAIL`** run
  (`gold.customer_360`, with an `errorMessage` facet) and an **`ABORT`** run
  (`gold.daily_revenue`).
- **Run DAGs** — silver/gold runs carry a `parent` run facet linking them to a
  daily orchestrator run.
- **`nominalTime`** windows on every run.
- **Dataset schema evolution** — `bronze.customers` gains a `marketing_opt_in`
  column mid-stream, producing **multiple dataset versions**.
- **Deep column lineage** (4+ hops, e.g. `public.customers.email` →
  `bronze.customers.email` → `silver.customers.email_hash` →
  `gold.customer_360.email_hash` → BI) with `DIRECT`/`INDIRECT` transforms,
  `AGGREGATION`, `JOIN`, `FILTER`, `GROUP_BY`, and **masking** edges.
- **Tags & PII propagation** — `pii` tags on source columns (`email`, `phone`,
  `user_id`, …) and dataset-level governance tags. The column-lineage chains let
  `GetTagDownstream` trace PII through the graph.
- **Out-of-band fact discovery** — a standalone `DatasetEvent` (no run)
  simulating a governance scanner asserting PII tags.
- **Job facets** — `sql`, `documentation`, `jobType`, `sourceCodeLocation`,
  `ownership`, `tags`.
- **Dataset facets** — `schema`, `dataSource`, `documentation`, `tags`,
  `dataQualityAssertions` (including a deliberately failing assertion),
  `columnLineage`.

## Running

The service needs Postgres. With a DSN exported:

```sh
export DATABASE_URL=postgres://user:pass@localhost:5432/lineage
just lineage          # serves on :8091 by default
just seed             # in another shell
```

`ingest.sh` honors `MARQUEZ_URL` / `LINEAGE_URL` to target a non-default host:

```sh
MARQUEZ_URL=http://localhost:9000 ./ingest.sh
./ingest.sh headwaters_demo.json     # one file only
```

It health-checks `/health` first (waiting up to 30s), then posts each file to
`POST /api/v1/lineage/batch` and prints `received` / `successful` / `failed`
counts. Per-event failures are reported, never fatal.

## Marquez reference data

`marquez_food_delivery.json` is the demo dataset the
[Marquez](https://github.com/MarquezProject/marquez) project ships in
`docker/metadata.template.json` and seeds via `docker/seed.sh`. It's vendored
here verbatim except that the `{{RUN_START_TIME}}` / `{{RUN_END_TIME_*}}`
templates are replaced with fixed timestamps (so the seed is static). It's a
single-namespace (`food_delivery`) graph of 13 ETL jobs and 13 `public.*`
datasets with `sql`/`documentation` job facets, `schema`/`dataSource`/
`columnLineage`/`dataQualityAssertions` dataset facets, and `nominalTime` run
facets — useful as a recognizable cross-check against the upstream reference
implementation. Licensed Apache-2.0 (same as this repo) by the Marquez project.
```
