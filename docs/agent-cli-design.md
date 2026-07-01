# Agent-facing `hw` CLI — question-verbs, design & handover

> How the `hw` CLI (`crates/headwaters-cli`) should grow its command surface so it
> is genuinely useful to *agents* operating a data platform, not just to humans:
> what altitude the commands sit at, what the first useful prototype is, and the
> research that backs it. The core decision is recorded as ADR
> [0014](adr/0014-agent-facing-cli-question-verbs.md); this is the longer handover
> that carries the research, the current-state gap, the command inventory, and the
> sources. Closely related: [CLI / server consolidation](cli-server-consolidation-design.md)
> (how `hw` relates to the server binary).

## Why

`hw` was introduced "for humans and agents" (commit `b83d4a3`) with a deliberate
agent idiom already in place — a three-mode `Render` trait (`table` / `json` /
`agent`), an interpreted `agent` envelope with `_next` follow-up hints, a `schema`
primer, semantic-verb framing (`trace`'s `"question"` key), structured JSON errors,
and stable exit codes. The question this doc answers is *how the command surface
grows from here*, given two pressures:

1. **The client can do far more than the CLI exposes** (gap table below).
2. **The scenarios are unbounded** — debugging, GDPR, governance, cost, drift — and
   we must not enumerate them as commands, nor collapse to thin endpoint wrappers.

## Current-state gap

`headwaters-client` (`crates/headwaters-client/src/client.rs`) exposes 16 read
methods; `hw` wires 5 commands. The unexposed methods are exactly the ones an agent
needs for real investigations:

| Client method | CLI today | Investigative question it unlocks |
|---|---|---|
| `list_namespaces` | `hw namespaces` | what estates exist? |
| `get_dataset` | `hw dataset get` | what is this table? |
| `get_lineage` | `hw lineage` / `hw trace` | what's around / feeds / is fed by this? |
| — (static) | `hw schema` | prime on the model |
| `list_datasets` | — | what tables are here? |
| `list_jobs` / `get_job` | — | what jobs are here / what does this job do? |
| `get_job_runs` | — | how has this job run (states, timing)? |
| `get_run_facets` | — | **why did this run fail** (`errorMessage`, `sql`)? |
| `search` | — | resolve a fuzzy name to a real `node_id` |
| `get_column_lineage` | — (dangling `_next`) | how is this column derived? |
| `list_tags` | — | what sensitivity labels exist? |
| `get_tag_downstream` | — | **where does this PII land downstream?** |
| `list_dataset_versions` | — | how has this schema drifted? |
| `list_events` | — | raw audit feed |
| `get_lineage_event_stats` / `get_asset_stats` | — | activity over time |

## Decision (see ADR 0014)

Grow the surface as **task-shaped question-verbs**: a small set of verbs that each
answer one recurring investigative question by composing backend calls and
returning the *answer* (interpreted, grouped, pruned), framed with a `"question"`
key and closed with runnable `_next` hints. Domain scenarios (GDPR, debugging,
governance) are **validation lenses**, not commands — the set is sufficient when
each scenario composes from 2–3 verbs. A thin substrate (`search`, the
`dataset`/`job` list+get grid) exists only so the surface is predictable and the
hints resolve.

## What good agent CLIs do (research distillation)

The principles below (sources at the end) shape the decision; most are already
partly present in `hw` — the design makes them consistent across every command.

- **Consolidate high-value workflows; don't wrap endpoints.** A few thoughtful
  verbs targeting real questions, each free to issue several backend calls, beat a
  large overlapping tool set. Anthropic's example: prefer one `get_customer_context`
  over separate `get_customer_by_id` / `list_transactions` / `list_notes`. This is
  the crux of the altitude decision.
- **Return high-signal, token-efficient output.** The *answer*, not raw dumps;
  interpret facets; drop low-value technical identifiers; group/summarize; cap and
  paginate anything unbounded. (`hw`'s `agent` mode already does this for the 5
  existing commands — e.g. lineage collapses to an adjacency list + summary, facets
  flatten to `columns`/`sql`.)
- **Self-describing, self-correcting errors + stable exit codes.** An error should
  tell the agent how to fix its next attempt; exit codes let it branch without
  parsing prose. (`hw` has `0`/`1`/`2`/`3` + JSON errors on stderr in `json`/`agent`
  modes.)
- **Structured output on success** with the fields needed to chain the next call
  (e.g. lead with `node_id`).
- **Predictable resource+verb structure** so an agent can guess an unseen command
  from a learned one (`dataset {list,get}`, `job {list,get}`).
- **Machine-readable self-description** so the agent primes once instead of
  re-deriving the model per response — the "capabilities" pattern, here folded into
  `hw schema`.
- **`_next` hints** turn each answer into a runnable next command; the CLI teaches
  the investigation. Every hint must name a command that exists.

## Command inventory (first prototype)

All reuse the existing `Render`/`agent`/`_next` machinery and existing
`HeadwatersClient` methods — no new client or server code.

### A. Governance question-verbs (the proving slice)

- **`hw tags`** — list the tag catalog (`list_tags`). The agent's entry point:
  "what sensitivity labels exist here?"
- **`hw exposure <tag>`** — *"Where does this sensitive data end up?"* Wraps
  `get_tag_downstream` and groups the reached fields by `namespace/dataset`:

  ```json
  {
    "question": "downstream exposure of tag pii",
    "tag": "pii",
    "datasets": [{ "ref": "analytics/orders", "fields": ["email", "phone"] }],
    "dataset_count": 1,
    "field_count": 2,
    "_next": [
      "hw dataset get analytics/orders",
      "hw column-lineage dataset:analytics/orders"
    ]
  }
  ```

  This single verb is the GDPR data-map / right-to-erasure answer: tag a source
  column `pii`, ask `hw exposure pii`, receive every downstream field to scrub.
- **`hw column-lineage <target>`** — *"How is this column derived?"* Wraps
  `get_column_lineage` (accepts `dataset:ns/name` or `datasetField:ns/name/field`),
  reshaping the `DATASET_FIELD` graph into an upstream derivation view. Makes the
  dangling `_next` hint real.

### B. Substrate (predictable grid + discovery)

- **`hw search <q>`** — `search` (`--kind`, `--namespace`, `--limit`); `agent`
  output leads with `node_id`s ready to paste into a verb.
- **`hw dataset list [namespace]`**, **`hw job list [namespace]`**,
  **`hw job get <ns> <name>`** — round out the grid; paginated with `total_count`
  surfaced so an agent knows when to page.

### C. Self-description

- **`hw schema`** gains a `commands` section (each command's question + example)
  and the new interpreted facets; stays static / no-server-call.

## Deferred (fast-follow once the pattern is proven)

- **Debugging-failures verb slice:** `hw runs <job>` / `hw why-failed <run>`
  (compose `get_job_runs` + `get_run_facets`, surface `errorMessage` + `sql`) and an
  impact verb. The natural second scenario; client support already exists.
- **`hw versions <dataset>`** (schema-drift), **`hw stats`** (activity), the
  **events feed** — remaining endpoint coverage, wrapped as questions.
- A generic per-field **column-downstream** traversal. The backend today offers
  only single-hop upstream `GetColumnLineage` and tag-scoped downstream via
  `GetTagDownstream`; tracing an *arbitrary* column downstream needs a new server
  endpoint/processor (ADR 0014's revisit trigger). `hw exposure` covers the
  governance case without it.

## Verification

See the executing plan for the full end-to-end check. The prototype's success
criterion is the governance chain against a server seeded with a `pii`-tagged
source column: `hw tags` → `hw exposure pii` → follow a `_next` into
`hw column-lineage …`, and `hw search` resolving a fuzzy name into a `node_id` that
feeds the verbs — all with no manual `node_id` construction.

## Sources (agent-CLI research)

- Anthropic — *Writing tools for agents*
  (https://www.anthropic.com/engineering/writing-tools-for-agents): consolidate
  workflows over endpoint wrappers; high-signal, token-efficient output;
  namespacing; self-correcting errors; right-size the tool set.
- Anthropic — *Effective context engineering for AI agents*
  (https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents).
- *Design Patterns for Agent-Ready CLIs* (linear-cli write-up,
  https://note.com/_kihonushi/n/nd8e57741e1d5): non-interactive design, examples in
  help, self-describing errors, `--json`, exit-code semantics, machine-readable
  capabilities, predictable resource+verb structure.
- *Command Line Interface Guidelines* (https://clig.dev/).
