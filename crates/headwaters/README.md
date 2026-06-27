# headwaters

An OpenLineage HTTP ingest service. It accepts [OpenLineage](https://openlineage.io)
events over HTTP, appends them to an **append-only Postgres event log**, and asynchronously
projects that log into normalized read tables that a [Marquez](https://marquezproject.ai)-compatible
REST API queries. It is the Rust successor to the original Go lineage ingest service.

The storage model is a hybrid CQRS split — a raw event log plus an async projection — described
in [ADR 0006](../../docs/adr/0006-hybrid-cqrs-postgres-storage.md).

## What it does

```mermaid
flowchart LR
    client(["Client"])

    subgraph ingest["Ingest path (never blocks on the write)"]
        direction LR
        http["http.rs<br/>(axum)"]
        conv["ingest/converter.rs"]
        buf["writer/buffered.rs"]
        pg["writer/postgres.rs"]
        http -->|"JSON → proto"| conv
        conv -->|"buffered"| buf
        buf -->|"flush"| pg
    end

    events[("events<br/>(append-only log,<br/>source of truth)")]
    tables[("namespaces / jobs /<br/>runs / datasets /<br/>lineage_edges<br/>(read tables)")]
    read["read/<br/>(axum)"]

    client -->|"POST /api/v1/lineage[/batch]"| http
    http -.->|"202 Accepted"| client
    pg -->|"append"| events
    events -->|"Projector (async)<br/>folds events → read tables"| tables

    tables -->|"graph / browse / stats"| read
    events -->|"event feed / run facets /<br/>column lineage"| read
    read -->|"GET /api/v1/…"| client
```

1. **Ingest** (`src/http.rs`) — `POST /api/v1/lineage` (one event) and
   `POST /api/v1/lineage/batch` (a JSON array). Handlers parse + enqueue, then return
   `202 Accepted`; they never block on the write. A `GET /health` liveness probe is also mounted.
2. **Convert** (`src/ingest/converter.rs`) — classifies each event as a Run / Job / Dataset
   event, validates `eventTime`, lifts the nested `columnLineage` facet into a typed field,
   and preserves the original wire bytes in `raw_json`. Events are held as zero-copy
   [`buffa`](https://crates.io/crates/buffa) views over owned bytes.
3. **Buffer** (`src/writer/buffered.rs`) — a background tokio task batches events and flushes
   on whichever comes first: a size threshold (`buffer_size`) or a time interval
   (`flush_interval_ms`). `enqueue` applies **backpressure** when the bounded channel is full
   (it does not drop events). On shutdown the channel drains before exit.
4. **Append** (`src/writer/postgres.rs`) — each flushed batch is shaped into `EventRow`s
   (`src/writer/row.rs`) and bulk-inserted into the append-only `events` table, the source of
   truth. The write goes through a pluggable `EventSink` trait (`src/writer/sink.rs`); Postgres
   is the only sink today.
5. **Project** (`src/projection/`) — an async `Projector` tails the `events` log by `seq` and
   folds each event into the normalized read tables (`namespaces`, `jobs`, `runs`, `datasets`,
   `lineage_edges`). Every fold is an idempotent, event-time-guarded upsert, so replaying the
   log reproduces the read tables (`rebuild` = truncate + reset cursor + re-fold). Facet
   processing runs through a backend-agnostic Mutation IR — see
   [ADR 0007](../../docs/adr/0007-mutation-ir-projection-pipeline.md).

Reads are **eventually consistent**: an event is visible to the read API once the projector has
folded it, at most one poll interval after ingestion — a trade a lineage browse UI tolerates in
exchange for an ingest path that never blocks on normalization.

## Read API (Marquez-compatible) — `src/read/`

The service serves a read-only, **Marquez-compatible** REST API so the upstream
[Marquez web UI](https://github.com/MarquezProject/marquez/tree/main/web) can visualize the
lineage with no UI code of our own. The Marquez UI is plain REST (no GraphQL); we implement the
subset its graph/browse views need, plus a few endpoints beyond Marquez (run facets, column
lineage, stats, tags):

| Endpoint | Returns |
|---|---|
| `GET /api/v1/namespaces` | distinct job/dataset namespaces |
| `GET /api/v1/jobs`, `GET /api/v1/namespaces/{ns}/jobs` | jobs with their input/output datasets |
| `GET /api/v1/namespaces/{ns}/jobs/{job}` | one job |
| `GET /api/v1/datasets`, `GET /api/v1/namespaces/{ns}/datasets` | datasets (standalone + job-referenced) |
| `GET /api/v1/namespaces/{ns}/datasets/{name}` | one dataset |
| `GET /api/v1/search?q=` | name substring search |
| `GET /api/v1/lineage?nodeId=&depth=` | the lineage graph (`WITH RECURSIVE` walk over `lineage_edges`) |
| `GET /api/v1/events/lineage` | the raw event feed |
| `GET /api/v1/jobs/runs/{run_id}/facets` | run facets from the raw event log |
| `GET /api/v1/column-lineage` | column-level lineage edges |
| `GET /api/v1/stats/lineage-events`, `GET /api/v1/stats/{asset}` | counts and stats |
| `GET /api/v1/tags`, `GET /api/v1/tags/{tag}/downstream` | tag inventory + PII/tag propagation |

The graph, browse, and stats endpoints query the projected read tables with indexed `sqlx`
statements (`src/read/queries.rs`); the event feed, run facets, and column-lineage endpoints read
the raw `events` log directly. A permissive CORS layer is applied so the browser-served UI can
call the API directly.

To run the UI against it, point the `marquezproject/marquez-web` image's proxy at this service
(`MARQUEZ_HOST` / `MARQUEZ_PORT`).

## Running

The service needs a Postgres database; it runs its own migrations (`migrations/`) on startup.

```sh
export DATABASE_URL=postgres://user:pass@localhost:5432/lineage
cargo run -p headwaters                 # or: cargo run -p headwaters -- path/to/config.toml
# then, from another shell:
curl -XPOST localhost:8091/api/v1/lineage \
  -H 'content-type: application/json' \
  --data-binary @crates/headwaters/examples/lineage/single/run-event.json
curl localhost:8091/health    # -> OK
```

### Configuration (`src/config.rs`)

Configuration is a TOML (or YAML/JSON) file, layered with defaults and environment overrides.
`Config::load` composes three sources, lowest precedence first:

1. **struct defaults** — every field has one, so an empty or partial file is valid;
2. **the config file** — passed as the binary's first argument, or via the `HEADWATERS_CONFIG`
   env var. A file requested explicitly that is missing or malformed is a hard error (a
   misconfigured deployment refuses to start rather than silently running on defaults);
3. **`HEADWATERS__*` environment overrides** — `__` separates nested keys, e.g.
   `HEADWATERS__PORT=9000` or `HEADWATERS__WRITER__BUFFER_SIZE=200`.

The Postgres DSN is then overlaid from `DATABASE_URL` if set, so the credential never needs to
live in the checked-in file. A resolvable DSN is required — the service fails fast at startup if
neither `postgres.url` nor `DATABASE_URL` is present.

```toml
port = 8091                 # HTTP listen port

[postgres]
# url = "postgres://user:pass@host:5432/lineage"   # prefer DATABASE_URL for the credential
pool_size = 10                 # connection pool size
projection_interval_ms = 500   # how often the projector polls the event log

[writer]
buffer_size = 100           # flush once this many events are buffered
flush_interval_ms = 500     # flush at least this often, even below buffer_size
channel_capacity = 1000     # bounded ingest channel depth (backpressure point)
```

`RUST_LOG` controls tracing verbosity (e.g. `RUST_LOG=headwaters=info`).

## Layout

```
src/
  http.rs              HTTP ingestion surface (axum router + handlers)
  config.rs            layered configuration + validation
  ingest/converter.rs  OpenLineage JSON → proto, column-lineage lifting
  writer/
    buffered.rs        async buffering + size/interval flush + backpressure
    sink.rs            EventSink trait + SinkError
    postgres.rs        Postgres `events` sink (the only sink today)
    row.rs             OpenLineage event view → EventRow (the `events` columns)
  projection/          async projector: folds `events` → read tables (Mutation IR + processors)
  read/                Marquez-compatible read API over the projected tables
  proto/lineage.v1.rs  generated by buffa — do not edit
migrations/            SQL schema migrations (run on startup)
tests/                 conformance, ingest round-trip, and read-API tests
examples/lineage/      sample OpenLineage event fixtures
```
