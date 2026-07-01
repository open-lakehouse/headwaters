# 0014 — Agent-facing CLI as task-shaped question-verbs

> Status: **Accepted** (2026-07). Shapes what commands the `hw` CLI
> (`crates/headwaters-cli`) grows. Builds on the CLI introduced in commit
> `b83d4a3` ("for humans and agents") and the read client from ADR
> [0013](0013-read-api-typed-enums-and-client-prep.md); the first proving slice
> rides the tag/PII propagation of ADR [0009](0009-tag-pii-propagation.md). The
> fuller inventory and the agent-CLI research live in the companion handover doc
> [`docs/agent-cli-design.md`](../agent-cli-design.md).

## Context

`hw` was introduced as a lineage-inspection CLI "for humans and agents" and
already carries a deliberate agent-oriented idiom: a three-mode `Render` trait
(`table` / `json` / `agent`), an `agent` envelope that flattens the high-value
facets, prunes noise, and appends `_next` follow-up hints (`_v: 1`); a `schema`
primer that needs no server call; semantic-verb framing (`trace` emits a literal
`"question"` key); structured JSON errors on stderr; and stable exit codes
(`0` ok, `1` server/io, `2` usage, `3` not-found).

Two forces make it time to decide *how the command surface grows*:

1. **A capability gap.** `headwaters-client` exposes 16 read methods; the CLI
   wires only 5 commands (`namespaces`, `dataset get`, `lineage`, `trace`,
   `schema`). Jobs, runs (with run states and the `errorMessage` run facet), run
   facets, search, column-lineage, tags + tag-downstream propagation, dataset
   versions, and stats are all reachable by the client but unreachable from the
   CLI. `dataset get`'s own `_next` hint already points at a `hw column-lineage`
   command that does not exist — an aspirational breadcrumb the surface has not
   caught up to.
2. **An altitude question.** The scenarios that motivate an agent reaching for
   lineage — debugging pipeline failures, GDPR/erasure data-mapping, governance
   reviews — are many. We do not want to enumerate them as commands (`hw gdpr`,
   `hw debug-failure`): that surface is unbounded and couples the CLI to a shifting
   catalogue of operator problems. Nor do we want the opposite failure — 16 thin
   commands that each wrap one endpoint 1:1, which the agent-tooling literature
   flags as a bloated, low-signal tool set.

## Decision

**The CLI's agent surface is a small set of task-shaped *question-verbs*.** Each
verb answers one recurring investigative question and may compose several backend
calls under the hood; it returns the *answer* (interpreted, grouped, pruned), not
a raw endpoint dump. This is the altitude between "one command per scenario" and
"one command per endpoint," and it extends the `trace` verb already in the tree.

Concretely:

- **Domain scenarios are validation lenses, not commands.** GDPR, debugging, and
  governance are how we judge whether the verb set is sufficient — each must be
  answerable by composing 2–3 verbs — not things that get their own command. The
  set is "done for a scenario" when the scenario composes cleanly.
- **Verbs frame the answer.** `agent` output leads with a `"question"` key (as
  `trace` does) so intent is legible to the model, and reshapes wire messages into
  the shape an agent reasons about (e.g. tag exposure grouped by dataset, not a
  flat field list).
- **Every answer teaches the next step.** `_next` hints are runnable commands that
  resolve to real commands using the canonical shorthand — the CLI walks the agent
  through an investigation rather than requiring it to re-derive node IDs.
- **`schema` is the machine-readable capabilities primer.** It gains a `commands`
  section (each command's one-line question + an example) so an agent primes once
  instead of re-deriving the model per response. It stays a static, no-server-call
  doc.
- **A thin substrate is still allowed where verbs and hints need it.** Discovery
  (`search`) and the resource+verb grid (`dataset {list,get}`, `job {list,get}`)
  exist so the surface is predictable and every `_next` hint resolves. These are
  not the point of the design, but they keep it coherent.

**First proving slice: the PII/GDPR governance scenario.** The backend already has
the exact primitive — ADR 0009's `GetTagDownstream` computes the transitive
downstream closure of a tag through `column_lineage_edges`. The flagship verb
`hw exposure <tag>` ("where does this sensitive data end up?") reshapes that
closure into the datasets and fields a `pii` tag reaches — the GDPR data-map /
right-to-erasure answer — supported by `hw tags` (discover labels) and
`hw column-lineage` (how an exposed field derives). That whole path is currently
unexposed in the CLI, so it is both high-value and a clean test of the pattern.

Rejected alternatives:

- **Scenario commands (`hw gdpr`, `hw debug-failure`).** Unbounded surface; bakes
  a shifting catalogue of operator problems into the CLI; hides the reusable
  primitives.
- **Endpoint-parity commands (16 thin wrappers).** Lower design cost but the
  "bloated, overlapping tool set" anti-pattern — low-signal raw dumps, and it
  pushes the composition work onto the agent on every investigation.

## Consequences

- **The verb set grows question-by-question, not endpoint-by-endpoint.** Adding a
  command means identifying a recurring question and the answer shape, then
  composing the client calls behind it. The debugging-failures slice
  (`hw runs` / `hw why-failed` over `get_job_runs` + `get_run_facets`, surfacing
  `errorMessage` + `sql`) is the natural second lens; the client already supports
  it.
- **Every new command ships all three render modes and resolving `_next` hints.**
  `json` stays the faithful wire message (a stable contract for scripts); `agent`
  is interpreted + pruned + `_next`; `table` is for humans. No `_next` hint may
  name a command that does not exist — landing `hw column-lineage` closes the one
  dangling hint today.
- **The governance answer is a pure read over existing projections** (ADR 0009's
  query-time closure); no new server or client code is required for the first
  slice — the design is realized entirely in the CLI's command + render layer.
- **The read client (ADR 0013) is the composition substrate.** Its exhaustive
  enums and additive filters are what let the verbs reshape confidently; the CLI
  never re-parses opaque JSON where a typed path exists.
- **Revisit trigger:** if a scenario cannot be answered by composing verbs because
  a primitive is missing (e.g. arbitrary per-field column-*downstream*, which the
  backend does not offer today — only single-hop upstream `GetColumnLineage` and
  tag-scoped downstream via `GetTagDownstream`), that is a signal to add a server
  endpoint/processor, not to add a scenario command.
