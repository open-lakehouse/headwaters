//! Integration tests for the OpenLineage DataFusion integration.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::SessionContext;
use datafusion_open_lineage::builder::{complete_event, fail_event, start_event};
use datafusion_open_lineage::config::OpenLineageConfig;
use datafusion_open_lineage::context::LineageContext;
use datafusion_open_lineage::event::{RunEvent, RunEventType};
use datafusion_open_lineage::extract::extract;
use datafusion_open_lineage::transport::{NoopTransport, Transport, TransportError};
use datafusion_open_lineage::{
    LineageContextProvider, OpenLineageClient, instrument_session_state,
    instrument_session_state_simple,
};
use serde_json::Value;
use uuid::Uuid;

/// A transport that records every event it receives, for assertions.
#[derive(Debug, Default, Clone)]
struct RecordingTransport {
    events: Arc<Mutex<Vec<RunEvent>>>,
}

#[async_trait]
impl Transport for RecordingTransport {
    async fn emit(&self, event: &RunEvent) -> Result<(), TransportError> {
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
}

/// A transport that always errors — to prove failures are swallowed.
#[derive(Debug)]
struct FailingTransport;

#[async_trait]
impl Transport for FailingTransport {
    async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
        Err(TransportError::Other("boom".to_string()))
    }
}

fn config() -> OpenLineageConfig {
    OpenLineageConfig {
        job_namespace: "test-ns".to_string(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// 1. Spec-conformant JSON
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_event_serializes_to_spec_shape() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let plan = ctx
        .state()
        .create_logical_plan("SELECT a, b FROM t")
        .await
        .unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();

    let lineage = extract(&optimized, &config());
    let event = start_event(
        Uuid::now_v7(),
        &lineage,
        &LineageContext::default(),
        &config(),
    );

    let json: Value = serde_json::to_value(&event).unwrap();
    // Envelope.
    assert_eq!(json["eventType"], "START");
    assert!(json["producer"].is_string());
    assert!(json["schemaURL"].is_string());
    // Every facet carries _producer + _schemaURL.
    let pe = &json["run"]["facets"]["processing_engine"];
    assert_eq!(pe["name"], "DataFusion");
    assert!(pe["_producer"].is_string());
    assert!(pe["_schemaURL"].is_string());
    // Job type facet.
    assert_eq!(
        json["job"]["facets"]["jobType"]["integration"],
        "DATAFUSION"
    );
}

// ---------------------------------------------------------------------------
// 2. Extraction: inputs, schema, column lineage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn extract_simple_projection() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    let plan = ctx
        .state()
        .create_logical_plan("SELECT a, b FROM t")
        .await
        .unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();

    let lineage = extract(&optimized, &config());
    assert_eq!(lineage.inputs.len(), 1, "one input table");
    assert_eq!(lineage.inputs[0].fields.len(), 2, "two schema fields");
}

#[tokio::test]
async fn schema_facet_uses_full_table_schema() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT, c INT) AS VALUES (1, 2, 3)")
        .await
        .unwrap();
    // Projection pushdown reduces the scan to column `a`, but the input
    // dataset's schema facet must still report the full table schema (a, b, c)
    // so the dataset's schema version doesn't flap across queries.
    let plan = ctx
        .state()
        .create_logical_plan("SELECT a FROM t")
        .await
        .unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();

    let lineage = extract(&optimized, &config());
    assert_eq!(lineage.inputs.len(), 1, "one input table");
    let mut names: Vec<&str> = lineage.inputs[0]
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["a", "b", "c"],
        "schema facet reports the full table schema, not the projected scan"
    );
}

#[tokio::test]
async fn self_join_dedupes_input() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    // A self-join scans `t` twice but it is a single input dataset.
    let plan = ctx
        .state()
        .create_logical_plan("SELECT l.a FROM t l JOIN t r ON l.a = r.a")
        .await
        .unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();

    let lineage = extract(&optimized, &config());
    assert_eq!(
        lineage.inputs.len(),
        1,
        "self-join is one deduped input: {:?}",
        lineage.inputs
    );
}

#[tokio::test]
async fn extract_insert_has_output() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE src (a INT) AS VALUES (1)")
        .await
        .unwrap();
    ctx.sql("CREATE TABLE dst (a INT) AS VALUES (0)")
        .await
        .unwrap();
    let plan = ctx
        .state()
        .create_logical_plan("INSERT INTO dst SELECT a FROM src")
        .await
        .unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();

    let lineage = extract(&optimized, &config());
    assert!(!lineage.inputs.is_empty(), "src is an input");
    assert_eq!(lineage.outputs.len(), 1, "dst is an output");
    assert!(lineage.outputs[0].name.name.contains("dst"));
}

// ---------------------------------------------------------------------------
// 3. Transport / client: START + COMPLETE share run id, failures swallowed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_and_complete_share_run_id() {
    let lineage = Default::default();
    let cx = LineageContext::default();
    let cfg = config();
    let run_id = Uuid::now_v7();

    let start = start_event(run_id, &lineage, &cx, &cfg);
    let complete = complete_event(run_id, &lineage, &cx, &cfg);
    let fail = fail_event(run_id, &lineage, &cx, &cfg, "kaboom");

    assert_eq!(start.run.run_id, complete.run.run_id);
    assert_eq!(start.run.run_id, fail.run.run_id);
    assert_eq!(start.event_type, RunEventType::Start);
    assert_eq!(complete.event_type, RunEventType::Complete);
    assert_eq!(fail.event_type, RunEventType::Fail);
    // Error facet present on FAIL only.
    let fail_json = serde_json::to_value(&fail).unwrap();
    assert_eq!(
        fail_json["run"]["facets"]["errorMessage"]["message"],
        "kaboom"
    );
}

#[tokio::test]
async fn failing_transport_does_not_panic() {
    let client = OpenLineageClient::new(Arc::new(FailingTransport));
    client.emit(start_event(
        Uuid::now_v7(),
        &Default::default(),
        &LineageContext::default(),
        &config(),
    ));
    // Give the worker a moment to process; the failing emit must be swallowed.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
}

#[tokio::test]
async fn try_new_errors_outside_tokio_runtime() {
    // Spawn a plain OS thread with no Tokio runtime; constructing there must
    // return an error rather than panic.
    let handle =
        std::thread::spawn(|| OpenLineageClient::try_new(Arc::new(NoopTransport)).is_err());
    assert!(
        handle.join().unwrap(),
        "try_new must error when no Tokio runtime is present"
    );
}

#[tokio::test]
async fn shutdown_drains_queued_events() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    for _ in 0..5 {
        client.emit(start_event(
            Uuid::now_v7(),
            &Default::default(),
            &LineageContext::default(),
            &config(),
        ));
    }
    // shutdown() awaits the drain task to completion, so all queued events are
    // delivered before it returns — no post-shutdown sleep needed.
    client.shutdown().await;

    assert_eq!(
        transport.events.lock().unwrap().len(),
        5,
        "all queued events delivered before shutdown returns"
    );
}

#[tokio::test]
async fn dropped_count_tracks_full_queue() {
    // A tiny queue and a transport that blocks forever so nothing drains: every
    // emit past the buffer is dropped and counted.
    #[derive(Debug)]
    struct BlockingTransport;
    #[async_trait]
    impl Transport for BlockingTransport {
        async fn emit(&self, _event: &RunEvent) -> Result<(), TransportError> {
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    let client = OpenLineageClient::builder()
        .transport(Arc::new(BlockingTransport))
        .queue_size(1)
        .build();
    // Far more than capacity (1 buffered + 1 in-flight); the rest are dropped.
    for _ in 0..20 {
        client.emit(start_event(
            Uuid::now_v7(),
            &Default::default(),
            &LineageContext::default(),
            &config(),
        ));
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(
        client.dropped_count() > 0,
        "a saturated queue must register dropped events"
    );
}

#[tokio::test]
async fn end_to_end_emits_start_and_complete() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    // Build an instrumented context from a fresh state, then create the table
    // inside it so there is exactly one catalog.
    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();

    let df = instrumented.sql("SELECT a FROM t").await.unwrap();
    let _ = df.collect().await.unwrap();

    // Allow the background emit worker to drain.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    let types: Vec<RunEventType> = events.iter().map(|e| e.event_type).collect();
    assert!(types.contains(&RunEventType::Start), "got START: {types:?}");
    assert!(
        types.contains(&RunEventType::Complete),
        "got COMPLETE: {types:?}"
    );
}

#[tokio::test]
async fn complete_fires_only_after_execution() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .sql("CREATE TABLE t (a INT) AS VALUES (1)")
        .await
        .unwrap();

    // Plan + physical plan, but do NOT consume the stream yet.
    let df = instrumented.sql("SELECT a FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Identify the run for THIS query by its START (setup CREATE TABLE produced
    // its own events). The select's run must have START but not yet COMPLETE.
    let run_id = {
        let events = transport.events.lock().unwrap();
        let start = events
            .iter()
            .rev()
            .find(|e| e.event_type == RunEventType::Start)
            .expect("a START for the select");
        let id = start.run.run_id;
        let completes = events
            .iter()
            .filter(|e| e.run.run_id == id && e.event_type == RunEventType::Complete)
            .count();
        assert_eq!(completes, 0, "COMPLETE must NOT fire before execution");
        id
    };

    // Now execute (drain all partitions) -> exactly one COMPLETE for this run.
    let task_ctx = instrumented.task_ctx();
    let partitions = plan.output_partitioning().partition_count();
    for p in 0..partitions {
        let mut stream = plan.execute(p, task_ctx.clone()).unwrap();
        use futures::StreamExt;
        while stream.next().await.is_some() {}
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let events = transport.events.lock().unwrap();
    let completes = events
        .iter()
        .filter(|e| e.run.run_id == run_id && e.event_type == RunEventType::Complete)
        .count();
    assert_eq!(completes, 1, "exactly one COMPLETE across partitions");
}

#[tokio::test]
async fn runtime_error_emits_fail() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .sql("CREATE TABLE t (a INT) AS VALUES (1), (2), (0)")
        .await
        .unwrap();

    // Division by zero surfaces as an error during stream execution, not at
    // planning time -> the run must report FAIL, not COMPLETE.
    let df = instrumented.sql("SELECT 10 / a AS r FROM t").await.unwrap();
    let result = df.collect().await;
    assert!(result.is_err(), "query should fail at runtime");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    // Correlate by the failing run (setup CREATE TABLE has its own run id).
    let fail = events
        .iter()
        .find(|e| e.event_type == RunEventType::Fail)
        .expect("a FAIL event");
    let run_id = fail.run.run_id;
    let for_run: Vec<RunEventType> = events
        .iter()
        .filter(|e| e.run.run_id == run_id)
        .map(|e| e.event_type)
        .collect();
    assert!(for_run.contains(&RunEventType::Start), "START: {for_run:?}");
    assert!(
        !for_run.contains(&RunEventType::Complete),
        "no COMPLETE on runtime failure: {for_run:?}"
    );
    // The FAIL event carries an errorMessage facet.
    let json = serde_json::to_value(fail).unwrap();
    assert!(json["run"]["facets"]["errorMessage"]["message"].is_string());
}

#[tokio::test]
async fn write_emits_output_statistics() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .sql("CREATE TABLE src (a INT) AS VALUES (1), (2), (3)")
        .await
        .unwrap();
    instrumented
        .sql("CREATE TABLE dst (a INT) AS VALUES (0)")
        .await
        .unwrap();

    // A write produces an output dataset; its COMPLETE event should carry
    // runtime row statistics harvested at end of execution.
    let df = instrumented
        .sql("INSERT INTO dst SELECT a FROM src")
        .await
        .unwrap();
    let _ = df.collect().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    // Find the COMPLETE event that has an output dataset (the INSERT run).
    let complete = events
        .iter()
        .find(|e| e.event_type == RunEventType::Complete && !e.outputs.is_empty())
        .expect("a COMPLETE with an output dataset");

    let json = serde_json::to_value(complete).unwrap();
    let stats = &json["outputs"][0]["outputFacets"]["outputStatistics"];
    assert!(
        stats["rowCount"].is_number(),
        "outputStatistics.rowCount present: {json}"
    );
    assert_eq!(stats["rowCount"], 3, "three rows written");
    assert!(stats["_producer"].is_string());
    assert!(stats["_schemaURL"].is_string());
}

#[tokio::test]
async fn single_input_read_emits_input_statistics() {
    use datafusion::prelude::ParquetReadOptions;

    // Scan metrics (rows/bytes scanned) are only populated by file sources, so
    // this test reads a real Parquet file (in-memory tables report nothing).
    let dir = std::env::temp_dir().join("ol_input_stats_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let parquet = dir.join("data.parquet");

    // Write a Parquet file via a throwaway context.
    let writer = SessionContext::new();
    writer
        .sql("CREATE TABLE src (a INT) AS VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();
    writer
        .sql(&format!(
            "COPY src TO '{}' STORED AS PARQUET",
            parquet.display()
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .register_parquet(
            "t",
            parquet.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

    let df = instrumented.sql("SELECT a FROM t").await.unwrap();
    let _ = df.collect().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    let complete = events
        .iter()
        .find(|e| e.event_type == RunEventType::Complete && !e.inputs.is_empty())
        .expect("a COMPLETE with an input dataset");

    let json = serde_json::to_value(complete).unwrap();
    let stats = &json["inputs"][0]["inputFacets"]["inputStatistics"];
    assert_eq!(stats["rowCount"], 5, "five rows read: {json}");
    assert!(stats["size"].is_number(), "bytes read present");
    assert!(stats["_producer"].is_string());
    assert!(stats["_schemaURL"].is_string());

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn multi_input_read_omits_input_statistics() {
    use datafusion::prelude::ParquetReadOptions;

    // With more than one input we cannot attribute a summed scan total to the
    // right source, so input statistics are intentionally omitted.
    let dir = std::env::temp_dir().join("ol_multi_input_test");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let writer = SessionContext::new();
    for (name, vals) in [("a", "(1), (2)"), ("b", "(3), (4), (5)")] {
        writer
            .sql(&format!("CREATE TABLE s (x INT) AS VALUES {vals}"))
            .await
            .unwrap();
        writer
            .sql(&format!(
                "COPY s TO '{}' STORED AS PARQUET",
                dir.join(format!("{name}.parquet")).display()
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        writer.sql("DROP TABLE s").await.unwrap();
    }

    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .register_parquet(
            "ta",
            dir.join("a.parquet").to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
    instrumented
        .register_parquet(
            "tb",
            dir.join("b.parquet").to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

    let df = instrumented
        .sql("SELECT ta.x FROM ta JOIN tb ON ta.x = tb.x")
        .await
        .unwrap();
    let _ = df.collect().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    let complete = events
        .iter()
        .find(|e| e.event_type == RunEventType::Complete && e.inputs.len() > 1)
        .expect("a COMPLETE with multiple input datasets");

    // No inputStatistics on any input (attribution would be ambiguous).
    for input in &complete.inputs {
        let has_stats = input
            .input_facets
            .as_ref()
            .map(|f| f.input_statistics.is_some())
            .unwrap_or(false);
        assert!(!has_stats, "multi-input run must omit inputStatistics");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 4. Context provider injects orchestration metadata
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct FixedContextProvider(LineageContext);

#[async_trait]
impl LineageContextProvider for FixedContextProvider {
    async fn context(
        &self,
        _state: &datafusion::execution::context::SessionState,
    ) -> LineageContext {
        self.0.clone()
    }
}

#[tokio::test]
async fn context_run_id_flows_to_event() {
    let fixed_run = Uuid::now_v7();
    let cx = LineageContext {
        run_id: Some(fixed_run),
        job_name: Some("airflow.my_dag.my_task".to_string()),
        ..Default::default()
    };

    let event = start_event(cx.run_id.unwrap(), &Default::default(), &cx, &config());
    assert_eq!(event.run.run_id, fixed_run);
    assert_eq!(event.job.name, "airflow.my_dag.my_task");
    // Provider trait is object-safe and usable.
    let provider: Arc<dyn LineageContextProvider> = Arc::new(FixedContextProvider(cx));
    let _ = format!("{provider:?}");
}

// ---------------------------------------------------------------------------
// 5. Extraction of DELETE / UPDATE / CTAS write operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn extract_delete_has_output() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT) AS VALUES (1), (2)")
        .await
        .unwrap();
    // DELETE is a DML write op -> the target table is an output dataset.
    let plan = ctx
        .state()
        .create_logical_plan("DELETE FROM t WHERE a = 1")
        .await
        .unwrap();
    let lineage = extract(&plan, &config());
    assert_eq!(lineage.outputs.len(), 1, "delete target is an output");
    assert!(lineage.outputs[0].name.name.contains("t"));
}

#[tokio::test]
async fn extract_ctas_has_output() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE src (a INT) AS VALUES (1), (2)")
        .await
        .unwrap();
    // CREATE TABLE AS SELECT -> the new table is an output, src an input.
    let plan = ctx
        .state()
        .create_logical_plan("CREATE TABLE dst AS SELECT a FROM src")
        .await
        .unwrap();
    let lineage = extract(&plan, &config());
    assert_eq!(lineage.outputs.len(), 1, "CTAS target is an output");
    assert!(!lineage.inputs.is_empty(), "src is an input");
}

// ---------------------------------------------------------------------------
// 6. Column lineage is emitted on outputs only (full coverage in
//    tests/column_lineage.rs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_only_query_has_no_column_lineage() {
    let ctx = SessionContext::new();
    ctx.sql("CREATE TABLE t (a INT, b INT) AS VALUES (1, 2)")
        .await
        .unwrap();
    // The spec defines `columnLineage` on output datasets; a pure SELECT has
    // none, so the event carries table-level lineage only — no `columnLineage`
    // facet anywhere (in particular not on inputs).
    let plan = ctx
        .state()
        .create_logical_plan("SELECT a + b AS c FROM t")
        .await
        .unwrap();
    let optimized = ctx.state().optimize(&plan).unwrap();
    let lineage = extract(&optimized, &config());

    let event = start_event(
        Uuid::now_v7(),
        &lineage,
        &LineageContext::default(),
        &config(),
    );
    let json = serde_json::to_string(&event).unwrap();
    assert!(
        !json.contains("columnLineage"),
        "no columnLineage key in emitted events: {json}"
    );
    // Table-level lineage is still present (the input dataset + its schema).
    assert_eq!(lineage.inputs.len(), 1, "input dataset present");
    let value: Value = serde_json::from_str(&json).unwrap();
    assert!(
        value["inputs"][0]["facets"]["schema"].is_object(),
        "schema facet present on input"
    );
}

// ---------------------------------------------------------------------------
// 7. Parent-run facet and SQL facet flow from the context into the event
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parent_run_facet_flows_to_start_event() {
    use datafusion_open_lineage::facets::{BaseFacet, ParentJob, ParentRun, ParentRunFacet};

    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let cfg = config();
    let cx = LineageContext {
        parent_run: Some(ParentRunFacet {
            base: BaseFacet::new(&cfg.producer, "1-0-0/ParentRunFacet.json"),
            run: ParentRun {
                run_id: "parent-run-1".to_string(),
            },
            job: ParentJob {
                namespace: "airflow".to_string(),
                name: "dag.task".to_string(),
            },
            root: None,
        }),
        sql: Some("SELECT a FROM t".to_string()),
        ..Default::default()
    };

    let base = SessionContext::new();
    let state = instrument_session_state(
        base.state(),
        client,
        Arc::new(FixedContextProvider(cx)),
        cfg,
    );
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .sql("CREATE TABLE t (a INT) AS VALUES (1)")
        .await
        .unwrap();
    let _ = instrumented
        .sql("SELECT a FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    let start = events
        .iter()
        .find(|e| e.event_type == RunEventType::Start)
        .expect("a START event");
    let json = serde_json::to_value(start).unwrap();
    // Parent facet populated from the context.
    assert_eq!(
        json["run"]["facets"]["parent"]["run"]["runId"], "parent-run-1",
        "parent run facet present: {json}"
    );
    // SQL job facet populated from the context.
    assert_eq!(
        json["job"]["facets"]["sql"]["query"], "SELECT a FROM t",
        "sql facet present: {json}"
    );
}

// ---------------------------------------------------------------------------
// 8. Stream cancellation (Drop before exhaustion) emits FAIL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dropped_stream_emits_fail() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .sql("CREATE TABLE t (a INT) AS VALUES (1), (2), (3)")
        .await
        .unwrap();

    let df = instrumented.sql("SELECT a FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    // Find the run id for this SELECT (its START).
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let run_id = {
        let events = transport.events.lock().unwrap();
        events
            .iter()
            .rev()
            .find(|e| e.event_type == RunEventType::Start)
            .expect("a START for the select")
            .run
            .run_id
    };

    // Start executing every partition, then drop the streams without draining.
    let task_ctx = instrumented.task_ctx();
    let partitions = plan.output_partitioning().partition_count();
    let mut streams = Vec::new();
    for p in 0..partitions {
        streams.push(plan.execute(p, task_ctx.clone()).unwrap());
    }
    drop(streams);
    drop(plan);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    let for_run: Vec<RunEventType> = events
        .iter()
        .filter(|e| e.run.run_id == run_id)
        .map(|e| e.event_type)
        .collect();
    assert!(
        for_run.contains(&RunEventType::Fail),
        "an abandoned (dropped) stream must report FAIL: {for_run:?}"
    );
    assert!(
        !for_run.contains(&RunEventType::Complete),
        "a dropped stream must not report COMPLETE: {for_run:?}"
    );
}

// ---------------------------------------------------------------------------
// 9. Terminal eventTime is refreshed at end of execution (not plan time)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn terminal_event_time_is_after_start() {
    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));

    let base = SessionContext::new();
    let state = instrument_session_state_simple(base.state(), client, config());
    let instrumented = SessionContext::new_with_state(state);
    instrumented
        .sql("CREATE TABLE t (a INT) AS VALUES (1), (2), (3)")
        .await
        .unwrap();

    let df = instrumented.sql("SELECT a FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();

    // Capture the START, then delay before executing so a plan-time terminal
    // timestamp would be measurably earlier than the real completion time.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let (run_id, start_time) = {
        let events = transport.events.lock().unwrap();
        let start = events
            .iter()
            .rev()
            .find(|e| e.event_type == RunEventType::Start)
            .expect("a START for the select");
        (start.run.run_id, start.event_time.clone())
    };

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let task_ctx = instrumented.task_ctx();
    let partitions = plan.output_partitioning().partition_count();
    for p in 0..partitions {
        let mut stream = plan.execute(p, task_ctx.clone()).unwrap();
        use futures::StreamExt;
        while stream.next().await.is_some() {}
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let events = transport.events.lock().unwrap();
    let complete = events
        .iter()
        .find(|e| e.run.run_id == run_id && e.event_type == RunEventType::Complete)
        .expect("a COMPLETE for the select");

    // Both are RFC3339; parse and compare. The terminal event must be strictly
    // later than START — a plan-time timestamp would make them ~equal.
    let start_ts = chrono::DateTime::parse_from_rfc3339(&start_time).unwrap();
    let complete_ts = chrono::DateTime::parse_from_rfc3339(&complete.event_time).unwrap();
    assert!(
        complete_ts > start_ts,
        "terminal eventTime ({}) must be after START eventTime ({})",
        complete.event_time,
        start_time
    );
}

// ---------------------------------------------------------------------------
// 10. An execute()-time error emits FAIL exactly once
// ---------------------------------------------------------------------------

/// A physical plan whose `execute()` always errors before producing a stream —
/// models an object-store auth / credential-vending failure at execution start.
#[derive(Debug)]
struct ExecErrorExec {
    properties: Arc<datafusion::physical_plan::PlanProperties>,
}

impl ExecErrorExec {
    fn new(partitions: usize) -> Self {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let properties = Arc::new(datafusion::physical_plan::PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { properties }
    }
}

impl datafusion::physical_plan::DisplayAs for ExecErrorExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "ExecErrorExec")
    }
}

impl datafusion::physical_plan::ExecutionPlan for ExecErrorExec {
    fn name(&self) -> &str {
        "ExecErrorExec"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        Err(datafusion::error::DataFusionError::Execution(
            "object store auth failed".to_string(),
        ))
    }
}

/// A trivial single-batch source with a configurable partition count, used to
/// prove the partition counter tracks `with_new_children` rewrites.
#[derive(Debug)]
struct OkExec {
    properties: Arc<datafusion::physical_plan::PlanProperties>,
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl OkExec {
    fn new(partitions: usize) -> Self {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};

        let schema: datafusion::arrow::datatypes::SchemaRef =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let properties = Arc::new(datafusion::physical_plan::PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self { properties, schema }
    }
}

impl datafusion::physical_plan::DisplayAs for OkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "OkExec")
    }
}

impl datafusion::physical_plan::ExecutionPlan for OkExec {
    fn name(&self) -> &str {
        "OkExec"
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        Ok(self)
    }
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        // An empty stream: completes immediately with no batches.
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::empty(),
            ),
        ))
    }
}

#[tokio::test]
async fn execute_error_emits_fail_exactly_once() {
    use datafusion_open_lineage::OpenLineageExec;
    use datafusion_open_lineage::builder::complete_event;

    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let cfg = config();
    let run_id = Uuid::now_v7();
    let complete = complete_event(
        run_id,
        &Default::default(),
        &LineageContext::default(),
        &cfg,
    );

    let inner: Arc<dyn datafusion::physical_plan::ExecutionPlan> = Arc::new(ExecErrorExec::new(2));
    let exec = OpenLineageExec::new(inner, client, complete, cfg.producer.clone());

    // Drive every partition; each `execute()` errors before yielding a stream.
    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    use datafusion::physical_plan::ExecutionPlan;
    for p in 0..2 {
        let res = exec.execute(p, task_ctx.clone());
        assert!(res.is_err(), "execute must propagate the error");
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    let fails = events
        .iter()
        .filter(|e| e.run.run_id == run_id && e.event_type == RunEventType::Fail)
        .count();
    let completes = events
        .iter()
        .filter(|e| e.run.run_id == run_id && e.event_type == RunEventType::Complete)
        .count();
    assert_eq!(fails, 1, "exactly one FAIL on execute()-time error");
    assert_eq!(completes, 0, "no COMPLETE on execute()-time error");
}

#[tokio::test]
async fn partition_count_change_emits_one_terminal() {
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_open_lineage::OpenLineageExec;
    use datafusion_open_lineage::builder::complete_event;

    let transport = RecordingTransport::default();
    let client = OpenLineageClient::new(Arc::new(transport.clone()));
    let cfg = config();
    let run_id = Uuid::now_v7();
    let complete = complete_event(
        run_id,
        &Default::default(),
        &LineageContext::default(),
        &cfg,
    );

    // Wrap a 1-partition plan, then rewrite the child to a 3-partition plan via
    // `with_new_children`. The terminal-event counter must follow the node that
    // actually executes (3 partitions), emitting exactly one COMPLETE.
    let inner: Arc<dyn ExecutionPlan> = Arc::new(OkExec::new(1));
    let exec = OpenLineageExec::new(inner, client, complete, cfg.producer.clone());
    let rewritten = Arc::clone(&exec)
        .with_new_children(vec![Arc::new(OkExec::new(3))])
        .unwrap();
    assert_eq!(
        rewritten.output_partitioning().partition_count(),
        3,
        "rewritten node reports the new partition count"
    );

    let ctx = SessionContext::new();
    let task_ctx = ctx.task_ctx();
    for p in 0..3 {
        let mut stream = rewritten.execute(p, task_ctx.clone()).unwrap();
        use futures::StreamExt;
        while stream.next().await.is_some() {}
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let events = transport.events.lock().unwrap();
    let completes = events
        .iter()
        .filter(|e| e.run.run_id == run_id && e.event_type == RunEventType::Complete)
        .count();
    let fails = events
        .iter()
        .filter(|e| e.run.run_id == run_id && e.event_type == RunEventType::Fail)
        .count();
    assert_eq!(
        completes, 1,
        "exactly one COMPLETE after a partition-count-changing rewrite"
    );
    assert_eq!(fails, 0, "no spurious FAIL");
}
