# Emitting OpenLineage — getting started

This guide shows how to emit OpenLineage lineage events from your code. Pick the
path that matches what you have:

| You have… | Use | Crate |
| --- | --- | --- |
| An Apache DataFusion `SessionState` | [`OpenLineage::builder()`](#datafusion) | `datafusion-open-lineage` |
| Events from somewhere else, or a custom delivery target | [a `Transport`](#custom) | `openlineage-client` |
| A running ingest service to receive events | the service itself | `headwaters` |

The emission side (the event model, the `Transport` seam, the non-blocking client)
lives in the engine-agnostic **`openlineage-client`** crate. **`datafusion-open-lineage`**
adds the DataFusion glue and re-exports that surface. The **`headwaters`** service is
the receiving end — it ingests events over HTTP and is not a dependency of either
emit crate (there is no OpenLineage spec for the read side, so it stays in the
service).

<a name="datafusion"></a>
## Instrumenting DataFusion

The simplest setup reads the standard OpenLineage environment and instruments a
session in one call:

```rust,no_run
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_open_lineage::OpenLineage;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let state = SessionStateBuilder::new_with_default_features().build();
let state = OpenLineage::builder().from_env()?.instrument(state);
let ctx = SessionContext::new_with_state(state);
// Run queries as usual — each emits START at plan time and COMPLETE/FAIL at end.
# let _ = ctx;
# Ok(())
# }
```

Set these before running (all optional; with `OPENLINEAGE_URL` unset the client is
a no-op, so instrumentation is safe to leave in):

| Variable | Meaning |
| --- | --- |
| `OPENLINEAGE_URL` | Base URL of the endpoint (e.g. `http://localhost:8091`) |
| `OPENLINEAGE_ENDPOINT` | Path appended to the URL (default `/api/v1/lineage`) |
| `OPENLINEAGE_API_KEY` | Bearer token, if the endpoint needs auth |
| `OPENLINEAGE_NAMESPACE` | Default job namespace |
| `OPENLINEAGE_TIMEOUT_MS` | Per-request transport timeout (default 30s) |
| `OPENLINEAGE_PARENT_*` | Parent run/job, to correlate with an orchestrator |

To inject per-query orchestration metadata (parent run, job name, custom facets,
SQL text), provide a `LineageContextProvider`:

```rust,no_run
# use std::sync::Arc;
# use datafusion::execution::context::SessionState;
# use datafusion_open_lineage::{OpenLineage, LineageContextProvider};
# fn run(state: SessionState, provider: Arc<dyn LineageContextProvider>) -> SessionState {
OpenLineage::builder().context(provider).from_env().unwrap().instrument(state)
# }
```

For advanced cases — sharing one client across many sessions, each with its own
context provider — use the lower-level `instrument_session_state` free function
(see `examples/e2e_pipeline/journey.rs` for a multi-stage pipeline doing exactly
this).

### Try it end to end

```sh
just dev      # Postgres + headwaters on :8091
just demo     # run the instrumented bronze → silver → gold pipeline
just ui-dev   # open the UI and explore the graph
```

Or a service-free dry run that logs each event as JSON:
`OPENLINEAGE_URL=console cargo run -p datafusion-open-lineage --example e2e_pipeline`.

<a name="custom"></a>
## Writing your own transport

How events are published is up to you. Implement `Transport` to target a Kafka
topic, a message queue, a file — anything:

```rust
use async_trait::async_trait;
use openlineage_client::{RunEvent, Transport, TransportError};

#[derive(Debug)]
struct KafkaTransport {
    // producer, topic, …
}

#[async_trait]
impl Transport for KafkaTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        let payload = serde_json::to_vec(event)?;
        // self.producer.send(self.topic, payload).await
        //     .map_err(|e| TransportError::Other(e.to_string()))?;
        let _ = payload;
        Ok(())
    }

    // Override `emit_batch` if the backend has a bulk path, and `flush` if it
    // buffers internally — the client calls `flush` on shutdown so the tail of
    // events isn't lost. Both have default implementations.
}
```

Then drive it directly, or hand it to the DataFusion builder:

```rust,no_run
# use std::sync::Arc;
# use openlineage_client::{OpenLineageClient, Transport};
# async fn drive(transport: Arc<dyn Transport>, event: openlineage_client::RunEvent) {
let client = OpenLineageClient::new(transport);
client.emit(event);          // non-blocking; never stalls your workload
client.shutdown().await;     // drain queued events + flush before exit
# }
```

```rust,no_run
# use std::sync::Arc;
# use datafusion::execution::context::SessionState;
# use datafusion_open_lineage::{OpenLineage, Transport};
# fn instrument(state: SessionState, transport: Arc<dyn Transport>) -> SessionState {
OpenLineage::builder().transport(transport).instrument(state)
# }
```

`emit` is non-blocking and drops on a full queue — lineage never applies
back-pressure to the host workload. See the `openlineage-client` crate docs for the
full contract.

## See also

- [OpenLineage on DataFusion — technical design](open-lineage-design.md) — how
  lineage is extracted from query plans.
- [ADR 0005 — planner vs. rule](adr/0005-openlineage-planner-vs-rule.md) — why the
  integration uses a `QueryPlanner` + `ExtensionPlanner`.
