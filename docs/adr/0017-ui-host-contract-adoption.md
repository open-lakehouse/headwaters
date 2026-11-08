# 0017 — The lineage UI packages adopt `@open-lakehouse/host`

> Status: **Accepted** (2026-11). Implements
> [#86](https://github.com/open-lakehouse/headwaters/issues/86), part of
> [luminescent-lakehouse#22](https://github.com/open-lakehouse/luminescent-lakehouse/issues/22).
> The contract itself is
> [trestle#277](https://github.com/open-lakehouse/trestle/issues/277), and the
> design behind it is in
> [luminescent-lakehouse#14](https://github.com/open-lakehouse/luminescent-lakehouse/issues/14).

## Context

`node/lineage-client` and `node/lineage-ui` were meant to be mounted by other
apps, but each host had to wire them up by hand. The desktop shell called
`registerTransport(createConnectTransport({ baseUrl: "", fetch: hostFetch("headwaters") }))`,
and it built both packages from a pinned source checkout, because neither was
published. Mangrove (`setDefaultUnityCatalogFetch`) and hydrofoil had their own
seams of the same kind. Trestle has since published one shared contract,
`@open-lakehouse/host`. A host registers a `LakehouseHost`. Its
`services.get(moduleId)` returns a `ServiceBinding { fetch, transport?, embedUrl?, placement }`
keyed by the trestle catalog module id (`"headwaters"`). Its `environment`
scope carries a `key` that changes on every switch. `createFakeHost` stands in
for a host in tests and Storybook.

## Decision

**Resolution on every call.** `clientTransport` (lineage-client) picks a
transport for every RPC, in this order:

1. a transport passed to `createReadClient(transport)`;
2. a `registerTransport` override, kept as a shim (the standalone app uses it
   to root RPCs at its base path);
3. the host's `headwaters` binding: its `transport`, or else Connect over its
   `fetch` with the read API at the binding root (the same thing the desktop
   built by hand);
4. the page's own origin, but only when there is no host at all.

If a host is present but doesn't bind `headwaters`, the call rejects with the
host's `ServiceUnavailableError`. Falling back to the page's origin there would
mean `tauri://localhost` in the desktop. `transportFor(host)` does the same
against a given host, and `ReadClientProvider` passes it the `<HostProvider>`
host when there is one. Because nothing is captured when the client is built,
the order of registration and an environment switch don't matter.

**Cache scoped by environment.** Every query key is
`[lineageQueryKey, scope, …]`. The provider's `scope` defaults to the host's
environment key, which it re-reads on every switch through
`useSyncExternalStore`. This is the host contract's remount-on-key rule, applied
inside the library, so the host doesn't have to clear its `QueryClient`.

**Required peer.** `@open-lakehouse/host` is a peer dependency of both
packages, and not an optional one: lineage-client imports `getHost()`
statically, so an optional peer would fail to resolve without it. It has to be
a peer rather than a dependency, because the registry is module state and the
app and every client must share one copy. While the host is `0.0.x` the peer is
pinned exactly, since a caret on `0.0.x` matches one version anyway.

**Published to GitHub Packages only**, renamed to
`@open-lakehouse/lineage-client` and `@open-lakehouse/lineage-ui`. The registry
requires the scope to match the org. Publishing uses the job's
`GITHUB_TOKEN`, with no npm token and no npmjs.com trusted-publisher bootstrap,
at the cost that consumers need an `.npmrc` scope line and a `read:packages`
token. The packages ship `dist/` (ESM and declarations) plus `src/`, which
Tailwind's `@source` scan in a host needs. Stories, tests and `src/testing/`
are left out.

**Versioned by placeholder crates**, the same way as trestle's npm packages:
`node/<package>/Cargo.toml` is `git_only`, its `version` is the npm version,
and release-plz tags `<package>-v<version>`. release-plz.yml then calls
publish-npm.yml for each tag, and `node/scripts/pack-package.mjs` stamps the
version into `package.json` (which stays at `0.0.0`) and pins lineage-ui's
dependency on lineage-client. A nested manifest takes the package out of the
`headwaters-ui` fileset, so `headwaters`' `changelog_include` lists both
packages next to `headwaters-ui`, and every UI change still rebuilds the image.

**A repo-specific export condition.** The workspace resolves the packages to
`src/` through `headwaters-source`, not the usual `source`. With `source`, Vite
and vitest would also have resolved `@open-lakehouse/host` to the `src/` it
publishes and compiled it without its tsconfig, which broke JSX.

## Consequences

- The desktop shell needs only `registerHost`, and its lineage `bind.ts`
  goes away when it moves to the published packages
  (luminescent-lakehouse#23). The rename breaks its source aliases until then.
- Storybook takes the same path as the desktop: a `createFakeHost` whose
  `headwaters` binding is an in-memory ConnectRPC router over the fixtures.
- The standalone app is unchanged in behaviour: there is no host, it keeps its
  `registerTransport`, and RPCs go to its own origin.
- Code that called `invalidateQueries` with a full key now has to include the
  scope, while `[lineageQueryKey]` still matches every query.
