# `@open-lakehouse/lineage-client`

The typed ConnectRPC client for headwaters' lineage read API
(`headwaters.read.v1.ReadService`), generated from the repository's `proto/`,
plus the read-model types. [`@open-lakehouse/lineage-ui`](../lineage-ui) is
built on it.

```bash
npm install @open-lakehouse/lineage-client @open-lakehouse/host
```

The package is on GitHub Packages, so the consumer's `.npmrc` maps the scope
there: `@open-lakehouse:registry=https://npm.pkg.github.com`, plus a token with
`read:packages`. `@open-lakehouse/host` is a peer dependency. The app and every
service client share the one host registry it holds.

## Usage

```ts
import { createReadClient } from "@open-lakehouse/lineage-client";

const client = createReadClient();
const { jobs } = await client.listJobs({ limit: 50 });
```

## How calls reach headwaters

The client never holds a base URL. Each call picks its transport again:

1. A transport passed explicitly: `createReadClient(transport)`.
2. The one installed with `registerTransport(transport)`. This is the explicit
   override for hosts that don't implement the host contract. The standalone
   app uses it to root RPCs at its base path. `registerTransport(null)` clears it.
3. The `headwaters` binding of the [`@open-lakehouse/host`][host] host,
   whether registered with `registerHost` or passed as `transportFor(host)`.
   The client uses the binding's ConnectRPC `transport` if it has one, and
   otherwise speaks Connect over its `fetch`, with the read API at the
   binding's root.
4. With no host at all, the page's own origin.

If a host is present but its environment doesn't bind `headwaters`, the call
rejects with `ServiceUnavailableError` (re-exported from the host). Because the
transport is resolved on every call, a client keeps working when a host or
transport is registered after it was created, and after an environment switch.

[host]: https://github.com/open-lakehouse/trestle/tree/main/node/host

## Development

The generated code is committed under `src/gen/`. Regenerate it with
`just ui-gen` after a proto change. Inside the workspace, imports resolve to
`src/` through the `headwaters-source` export condition. `npm run build` emits the
published `dist/`.
