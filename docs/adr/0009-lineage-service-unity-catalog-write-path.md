# 0009 — lineage-service Unity Catalog write path

> Status: **Proposed** (2026-06). Design for `crates/lineage-service`; not yet
> implemented. Relates to
> [`0004-per-session-credential-isolation.md`](0004-per-session-credential-isolation.md)
> and the read-path UC integration in `crates/hydrofoil/src/catalog/unity.rs`.

## Context

`lineage-service` writes OpenLineage events to a Delta **events table**. Today its
`DeltaWriter` (`crates/lineage-service/src/writer/delta.rs`) is fully decoupled from Unity
Catalog: it holds a raw `table_uri` plus a flat `storage_options` map, resolves the URI with
`ensure_table_uri`, and writes via the delta-rs builder. Credentials come from static `AWS_*`
environment variables.

We want the events table to be a **Unity Catalog-managed Delta table** — resolving its
location, and ideally its write credentials, from UC OSS the same way the query engine
(`hydrofoil`) resolves them for reads. Three things must hold:

1. **Write credentials must be vendable.** Historically there was doubt about whether UC OSS
   could vend *write*-scoped temporary credentials (a Delta commit needs `PutObject`). This
   was the single biggest risk.
2. **The writer must stay lean.** `lineage-service` is a standalone binary; it should not
   take a dependency on the whole `hydrofoil` crate and its per-query session machinery just
   to write one table.
3. **The seam must keep local/test setups simple.** The existing raw-URI behavior must remain
   available without a UC server.

Verification against the pinned `unitycatalog-rs` revision
(`8e86500eb5a64e8c811f440cccfa9b8138c83550`) retires risk (1):
`UnityObjectStoreFactory::for_table(name, TableOperation::ReadWrite)` vends write
credentials, and the server adds `s3:PutObject`/`s3:DeleteObject` to the STS session policy
for the `ReadWrite` operation (`server/src/services/credential_vending.rs`). The
auto-refreshing `UCStore::root()` is a bucket-rooted `Arc<dyn ObjectStore>`, and the
delta-rs fork exposes `DeltaTableBuilder::with_storage_backend(Arc<dyn ObjectStore>, Url)` to
inject it. The read-path resolver hardcodes `TableOperation::Read`, so the writer must call
`for_table(..., ReadWrite)` itself rather than reusing that resolver.

## Decision

- **Introduce a `TableLocator` seam** that resolves `(location, object_store)` for the events
  table, and make `DeltaWriter` agnostic to how they are obtained:
  - `StaticLocator` — today's behavior (raw URI + storage options); the default for local and
    test setups.
  - `UnityLocator` — resolves the table by `catalog.schema.table` via `unitycatalog-client`
    (`Table.storage_location`), vends write credentials with
    `UnityObjectStoreFactory::for_table(name, TableOperation::ReadWrite)`, and injects
    `store.root()` into the Delta builder via `with_storage_backend`.
- **Add only the minimal UC crates** to `lineage-service`: `unitycatalog-client` (resolution)
  and `unitycatalog-object-store` (write-credential vending). Do **not** depend on
  `hydrofoil` or `datafusion-unitycatalog` (the latter's value is the read-only DataFusion
  resolver, which the writer does not want).
- **Resolve once at startup, refresh on a slow timer.** `build_sinks` is already `async`;
  resolve the location + vend the store at boot, cache it, and re-vend on credential expiry
  (the `UCCredentialProvider` token cache handles refresh transparently for a long-running
  writer).
- **Keep `DeltaOps`/`DeltaTable::write` for commits** — no custom `LogStore`. Hydrofoil's
  `DataFusionLogStore` exists to run the kernel on a *shared session* `TaskContext` and
  preserve *per-session* credentials across principals; the writer has one principal and its
  own runtime, so plain appends through a properly-credentialed store are correct.
- **v1 assumes the operator pre-creates the UC table.** The writer resolves an existing table
  and fails fast with a bootstrap hint if it 404s. Auto-creating + registering a table from a
  write-only daemon (MANAGED-vs-EXTERNAL, location allocation, schema registration, extra
  privileges) is out of scope.
- **v1 targets a direct-commit table.** Plain `DeltaOps::write` puts `_delta_log` entries
  straight to storage; it does not route through the UC commit coordinator
  (`server/src/api/commits.rs`). The events table must therefore be an external /
  non-coordinated Delta table — which matches hydrofoil's own direct-commit behavior.

### Deviation from ADR 0004 (per-session credential isolation)

ADR 0004 mandates a fresh `RuntimeEnv` per session so vended credentials cannot leak across
*principals*. `lineage-service` is a **single-principal service-account writer** — there is
no second principal to leak to — so one long-lived `UnityObjectStoreFactory` and one cached
`ReadWrite` store is correct and does not violate the isolation invariant. This deviation is
intentional and scoped to the writer.

## Consequences

- Write-credential vending is confirmed feasible with no upstream/fork change; the residual
  risk is **commit mode**, not credentials — a coordinated MANAGED events table would diverge
  under plain `DeltaOps::write`. Mitigation: require a direct-commit table for v1 and document
  it. Coordinated-commit support is a future phase needing a UC-aware `LogStore` that POSTs to
  the commit coordinator.
- The `TableLocator` abstraction keeps the `TableSink` trait untouched and makes the writer
  fully testable with `StaticLocator` against a temp dir.
- The `with_storage_backend` builder signature should be re-confirmed against the delta-rs
  fork at implementation time; the runtime object-store-registry route (as the read path uses)
  is the fallback if the builder seam differs.
