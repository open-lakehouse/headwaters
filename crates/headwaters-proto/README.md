# headwaters-proto

Generated protobuf types and ConnectRPC stubs for the [Headwaters](https://github.com/open-lakehouse/headwaters)
lineage service.

Two proto packages:

- `lineage::v1` — the OpenLineage-aligned event/facet model and ingest shapes.
- `headwaters::read::v1` — the read API DTOs and the `ReadService`
  request/response messages.

The ConnectRPC facade in `connect_gen` carries both a server-side `ReadService`
trait and a `ReadServiceClient`. The runtime they need is feature-gated so a
consumer pulls only its side:

| Feature      | Enables                                                        |
|--------------|----------------------------------------------------------------|
| `server`     | hyper/axum server runtime for mounting `ReadService`           |
| `client`     | hyper client runtime for `ReadServiceClient` (`HttpClient`)    |
| `client-tls` | TLS for the client transport                                   |

The generated `*.rs` are committed and produced by `just proto-gen`; do not
hand-edit them.
