# Headwaters documentation

- [Architecture Decision Records](adr/README.md) — the numbered design decisions
  behind the lineage crates (run-id correlation, column lineage, storage, the
  projection pipeline, tags, PII propagation).
- [OpenLineage on DataFusion — technical design](open-lineage-design.md) — how the
  `datafusion-open-lineage` crate hooks into DataFusion and extracts lineage.
- [Marquez compatibility & divergences](marquez-compatibility.md) — where
  `lineage-service` matches, diverges from, and goes beyond the Marquez reference
  implementation.
