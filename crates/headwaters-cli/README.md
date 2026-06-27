# headwaters-cli (`hw`)

A command-line tool to inspect a [Headwaters](https://github.com/open-lakehouse/headwaters)
data-lineage estate — built for both humans and LLM agents.

```console
$ hw namespaces
$ hw dataset get snowflake://analytics gold.customer_360
$ hw lineage dataset:analytics:orders --direction up --depth 3
$ hw trace dataset:analytics:orders --direction up
$ hw schema          # prime an agent on the data model (no server call)
```

## Output modes

Every command takes `-o/--output`:

- **`table`** (default) — human-readable tables and trees.
- **`json`** — the faithful wire message; a stable contract for scripts (`| jq`).
- **`agent`** — the same data interpreted and pruned for an LLM: known facets
  flattened into plain fields, lineage graphs collapsed to an adjacency list +
  summary (the bulky per-node entity blobs dropped), unknown facets reduced to a
  name list, and `_next` follow-up commands suggested. Carries `"_v": 1`.

## Targets

Graph commands accept a full nodeId (`dataset:<ns>:<name>`) or a `kind:<ns>/<name>`
shorthand. A `:` after the kind marks a full nodeId, so URI namespaces
(`dataset:snowflake://analytics:orders`) are handled correctly.

## Configuration

- `--server` / `HW_SERVER` — base URL (default `http://localhost:8091`).
- `-o` / `HW_OUTPUT` — output mode.
- `--raw-facets` — pass opaque facet bags through instead of interpreting them.

## Exit codes

`0` success · `1` server/transport error · `2` usage error · `3` not found. In
`json`/`agent` modes, errors are also emitted as a structured object on stderr.
