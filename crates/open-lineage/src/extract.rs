//! Extract table-level lineage (input/output datasets + schema) from a
//! DataFusion [`LogicalPlan`].
//!
//! Follows the same `TreeNodeVisitor`-over-`LogicalPlan` shape the Cedar policy
//! integration uses. Run this on the *optimized* plan so projections/filters are
//! pushed down to the scans.
//!
//! Column-level lineage is resolved separately by [`crate::column`] (a
//! positional bottom-up walk) and attached to the output datasets here.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::error::Result;
use datafusion::logical_expr::{DdlStatement, LogicalPlan, WriteOp};
use datafusion::sql::TableReference;

use crate::column::{ResolvedColumns, resolve_output_columns};
use crate::config::OpenLineageConfig;
use crate::facets::{
    BaseFacet, ColumnLineageDatasetFacet, DatasetFacets, FieldLineage, InputField,
    SchemaDatasetFacet, SchemaField, Transformation, TransformationType,
};
use crate::naming::DatasetName;

const SCHEMA_FACET: &str = "1-2-0/SchemaDatasetFacet.json";
const COLUMN_LINEAGE_FACET: &str = "1-2-0/ColumnLineageDatasetFacet.json";

/// What a query reads and writes.
#[derive(Debug, Default)]
pub struct QueryLineage {
    pub inputs: Vec<InputTable>,
    pub outputs: Vec<OutputTable>,
    pub sql: Option<String>,
}

#[derive(Debug)]
pub struct InputTable {
    pub name: DatasetName,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug)]
pub struct OutputTable {
    pub name: DatasetName,
    /// The output table's columns, emitted as a `schema` dataset facet so the
    /// written table shows its columns in the lineage graph. Empty when the
    /// writer can't resolve a schema.
    pub fields: Vec<SchemaField>,
    /// Output field name -> source columns, when soundly resolvable.
    pub column_lineage: Option<ResolvedColumns>,
}

/// Extract [`QueryLineage`] from an (ideally optimized) logical plan.
pub fn extract(plan: &LogicalPlan, config: &OpenLineageConfig) -> QueryLineage {
    let mut visitor = LineageVisitor {
        config,
        inputs: Vec::new(),
        outputs: Vec::new(),
    };
    // The visitor never returns an error; ignore the traversal Result.
    let _ = plan.visit(&mut visitor);

    let mut outputs = visitor.outputs;
    if !outputs.is_empty()
        && let Some(resolved) = resolve_output_columns(plan, config)
    {
        // A statement writes (at most) one dataset; the resolved root map
        // describes exactly its fields.
        for output in &mut outputs {
            output.column_lineage = Some(resolved.clone());
        }
    }

    QueryLineage {
        inputs: visitor.inputs,
        outputs,
        sql: None,
    }
}

/// Map a table reference to its OpenLineage dataset name.
///
/// A bare TableScan carries only the qualified reference, not a storage
/// location. Use the qualified name under the configured namespace; the host
/// integration can enrich with a physical location + symlinks facet. Shared by
/// the table-level visitor and the column resolver so the two can never
/// disagree on dataset identity.
pub(crate) fn dataset_for(table_ref: &TableReference, config: &OpenLineageConfig) -> DatasetName {
    DatasetName::from_table_ref(&config.job_namespace, &table_ref.to_string())
}

/// Map Arrow fields to OpenLineage [`SchemaField`]s (name + type string). Shared
/// by input scans and output writers so a dataset's schema facet is consistent
/// however it's produced.
pub fn schema_fields(fields: &datafusion::arrow::datatypes::Fields) -> Vec<SchemaField> {
    fields
        .iter()
        .map(|f| SchemaField {
            name: f.name().to_string(),
            type_: f.data_type().to_string(),
            description: None,
        })
        .collect()
}

struct LineageVisitor<'a> {
    config: &'a OpenLineageConfig,
    inputs: Vec<InputTable>,
    outputs: Vec<OutputTable>,
}

impl LineageVisitor<'_> {
    fn dataset_for(&self, table_ref: &TableReference) -> DatasetName {
        dataset_for(table_ref, self.config)
    }
}

impl TreeNodeVisitor<'_> for LineageVisitor<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            // Skip scans of the `information_schema` virtual catalog: it is
            // DataFusion's metadata surface, not a real dataset, so reporting it
            // as a lineage input only adds noise. Treating these scans as
            // non-inputs is also what lets the planner suppress pure-metadata
            // queries (no inputs + no outputs => no events).
            LogicalPlan::TableScan(scan)
                if scan
                    .table_name
                    .schema()
                    .is_some_and(|s| s.eq_ignore_ascii_case("information_schema")) => {}
            LogicalPlan::TableScan(scan) => {
                let dataset = self.dataset_for(&scan.table_name);
                // Report the *full* table schema, not the projected scan schema:
                // after projection pushdown `SELECT a FROM t` would otherwise
                // report `t` as having only column `a`, causing the dataset's
                // schema version to flap between queries.
                let fields: Vec<SchemaField> = scan
                    .source
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| SchemaField {
                        name: f.name().to_string(),
                        type_: f.data_type().to_string(),
                        description: None,
                    })
                    .collect();
                // Dedupe by `(namespace, name)`: a self-join scans the same
                // table twice but it is a single input dataset.
                if !self
                    .inputs
                    .iter()
                    .any(|i| i.name.namespace == dataset.namespace && i.name.name == dataset.name)
                {
                    self.inputs.push(InputTable {
                        name: dataset,
                        fields,
                    });
                }
            }
            LogicalPlan::Dml(dml) => match dml.op {
                WriteOp::Insert(_) | WriteOp::Update | WriteOp::Delete | WriteOp::Ctas => {
                    self.outputs.push(OutputTable {
                        name: self.dataset_for(&dml.table_name),
                        // The write target's full table schema -> the output's columns.
                        fields: schema_fields(dml.target.schema().fields()),
                        column_lineage: None,
                    });
                }
                WriteOp::Truncate => {}
            },
            LogicalPlan::Ddl(ddl) => match ddl {
                DdlStatement::CreateExternalTable(cmd) => {
                    self.outputs.push(OutputTable {
                        name: self.dataset_for(&cmd.name),
                        fields: schema_fields(cmd.schema.as_arrow().fields()),
                        column_lineage: None,
                    });
                }
                // `CREATE TABLE ... AS SELECT` lowers to CreateMemoryTable; the
                // new table is the output dataset (the SELECT's scans are its
                // inputs, picked up by the TableScan arm).
                DdlStatement::CreateMemoryTable(cmd) => {
                    self.outputs.push(OutputTable {
                        name: self.dataset_for(&cmd.name),
                        fields: schema_fields(cmd.input.schema().as_arrow().fields()),
                        column_lineage: None,
                    });
                }
                _ => {}
            },
            // Nodes we don't yet derive lineage from. Warn so coverage gaps are
            // visible rather than silently dropped.
            other => {
                tracing::trace!(
                    target: "openlineage",
                    node = other.display().to_string(),
                    "no lineage extraction for plan node"
                );
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Build the [`DatasetFacets`] for an input table: its schema facet.
///
/// Column lineage never appears on inputs — the spec defines the facet on
/// output datasets, keyed by output field.
pub fn input_dataset_facets(input: &InputTable, config: &OpenLineageConfig) -> DatasetFacets {
    let schema = SchemaDatasetFacet {
        base: BaseFacet::new(&config.producer, SCHEMA_FACET),
        fields: input.fields.clone(),
    };

    DatasetFacets {
        schema: Some(schema),
        ..Default::default()
    }
}

/// Build the [`DatasetFacets`] for an output table: its column-lineage facet,
/// when the resolution produced one.
///
/// Each output field lists its direct sources; the statement-wide indirect
/// influences (filter/join/group/sort keys) are appended to every field's
/// `inputFields`, matching how the OpenLineage Spark integration emits them.
pub fn output_dataset_facets(output: &OutputTable, config: &OpenLineageConfig) -> DatasetFacets {
    // Schema facet: emit the output table's columns (when known) so the written
    // dataset shows its columns in the graph — independent of whether column
    // lineage resolved.
    let schema = (!output.fields.is_empty()).then(|| SchemaDatasetFacet {
        base: BaseFacet::new(&config.producer, SCHEMA_FACET),
        fields: output.fields.clone(),
    });

    let Some(resolved) = &output.column_lineage else {
        return DatasetFacets {
            schema,
            ..Default::default()
        };
    };
    if resolved.fields.is_empty() {
        // No column lineage resolvable (e.g. all-literal INSERT / bulk ingest):
        // still emit the schema facet so the dataset carries its columns.
        return DatasetFacets {
            schema,
            ..Default::default()
        };
    }

    let direct = |subtype: &str| Transformation {
        type_: TransformationType::Direct,
        subtype: Some(subtype.to_string()),
        description: String::new(),
        masking: false,
    };
    let indirect = |subtype: &str| Transformation {
        type_: TransformationType::Indirect,
        ..direct(subtype)
    };

    let fields = resolved
        .fields
        .iter()
        .map(|(field, sources)| {
            // One InputField per source, carrying its direct transformation
            // plus any statement-wide indirect influences on the same source.
            let mut input_fields: Vec<InputField> = sources
                .direct
                .iter()
                .map(|(source, kind)| {
                    let mut transformations = vec![direct(kind.subtype())];
                    if let Some(kinds) = resolved.indirect.get(source) {
                        transformations.extend(kinds.iter().map(|k| indirect(k.subtype())));
                    }
                    InputField {
                        namespace: source.dataset.namespace.clone(),
                        name: source.dataset.name.clone(),
                        field: Some(source.column.clone()),
                        transformations,
                    }
                })
                .collect();
            // Indirect-only sources (e.g. a filter column the output never
            // carries) still influence every output field.
            input_fields.extend(
                resolved
                    .indirect
                    .iter()
                    .filter(|(source, _)| !sources.direct.contains_key(source))
                    .map(|(source, kinds)| InputField {
                        namespace: source.dataset.namespace.clone(),
                        name: source.dataset.name.clone(),
                        field: Some(source.column.clone()),
                        transformations: kinds.iter().map(|k| indirect(k.subtype())).collect(),
                    }),
            );
            (field.clone(), FieldLineage { input_fields })
        })
        .collect();

    DatasetFacets {
        schema,
        column_lineage: Some(ColumnLineageDatasetFacet {
            base: BaseFacet::new(&config.producer, COLUMN_LINEAGE_FACET),
            fields,
            dataset: Vec::new(),
        }),
        ..Default::default()
    }
}
