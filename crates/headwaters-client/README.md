# headwaters-client

An async Rust client for the [Headwaters](https://github.com/open-lakehouse/headwaters)
read API — browse namespaces, jobs, datasets, and runs; walk the lineage and
column-lineage graph; search; read tags, run facets, and activity stats.

```rust,no_run
use headwaters_client::HeadwatersClient;

#[tokio::main]
async fn main() -> Result<(), headwaters_client::Error> {
    let client = HeadwatersClient::connect("http://localhost:8091")?;

    for ns in client.list_namespaces().await?.namespaces {
        println!("{}", ns.name);
    }

    let node = headwaters_client::dataset_node_id("analytics", "orders");
    let graph = client.get_lineage(&node, 2).await?;
    println!("{} reachable nodes", graph.graph.len());
    Ok(())
}
```

- `HeadwatersClient::connect(url)`, `::from_env()` (`HEADWATERS_URL` /
  `HEADWATERS_TOKEN` / `HEADWATERS_TIMEOUT_MS`), or `::new(HeadwatersConfig)`.
- One method per read RPC, returning owned response messages from
  [`headwaters-proto`](https://docs.rs/headwaters-proto).
- Open-ended `facets` / `data` / `fields` / `events` fields are
  `google.protobuf.Struct`; `struct_to_json` converts one to a
  `serde_json::Value`.

Speaks ConnectRPC over HTTP/1.1. Only `http://` base URLs are supported today;
TLS is a planned follow-up.
