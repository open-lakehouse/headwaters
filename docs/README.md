# Headwaters documentation

- [Emitting OpenLineage — getting started](emitting-lineage.md) — how to emit
  lineage events from DataFusion or any other source, and how to write a custom
  `Transport`.
- [Architecture Decision Records](adr/README.md) — the numbered design decisions
  behind the lineage crates (run-id correlation, column lineage, storage, the
  projection pipeline, tags, PII propagation).
- [OpenLineage on DataFusion — technical design](open-lineage-design.md) — how the
  `datafusion-openlineage` crate hooks into DataFusion and extracts lineage.
- [Envoy → PostgreSQL OpenLineage integration — technical design](envoy-postgres-lineage-design.md)
  — feasibility and design for a custom `proxy-wasm` filter that emits lineage from
  Postgres traffic through an Envoy proxy ([ADR 0011](adr/0011-envoy-postgres-lineage-via-proxy-wasm.md)).
- [Marquez compatibility & divergences](marquez-compatibility.md) — where
  `headwaters` matches, diverges from, and goes beyond the Marquez reference
  implementation.
