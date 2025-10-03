use crate::memory::storage::index::PlannedPatternScan;
use crate::memory::storage::predicate_pushdown::MemStoragePredicateExpr;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_err, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use itertools::Itertools;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// The physical operator for evaluating a quad pattern against a [MemQuadStorage](crate::memory::MemQuadStorage).
#[derive(Debug)]
pub struct MemQuadPatternExec {
    /// The execution properties of this operator.
    plan_properties: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// A [PlannedPatternScan] that represents a scan that is about to be executed.
    planned_scan: PlannedPatternScan,
}

impl MemQuadPatternExec {
    /// Creates a new [MemQuadPatternExec].
    pub fn new(schema: SchemaRef, stream: PlannedPatternScan) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative); // We wrap our stream in `cooperative`

        Self {
            plan_properties,
            planned_scan: stream,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }
}

impl ExecutionPlan for MemQuadPatternExec {
    fn name(&self) -> &str {
        "MemQuadPatternExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            return Ok(self);
        }

        internal_err!("QuadPatternExec does not have children")
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("Only partition 0 is supported for now.");
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let result = self.planned_scan.clone().create_stream(baseline_metrics);
        if result.schema() != self.schema() {
            return exec_err!("Unexpected schema for quad pattern stream.");
        }

        Ok(result)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if !matches!(phase, FilterPushdownPhase::Pre) {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        let parent_filters = child_pushdown_result
            .parent_filters
            .clone()
            .into_iter()
            .map(|f| {
                let rewritten =
                    MemStoragePredicateExpr::try_from(self.schema().as_ref(), &f.filter);
                (f.filter, rewritten)
            })
            .collect::<Vec<_>>();

        let results = parent_filters
            .iter()
            .map(|(_, rewritten)| match rewritten {
                None => PushedDown::No,
                Some(_) => PushedDown::Yes,
            })
            .collect::<Vec<_>>();

        // Don't create a new node if no filters were pushed down
        if results.iter().all(|r| matches!(r, PushedDown::No)) {
            return Ok(FilterPushdownPropagation {
                filters: results,
                updated_node: None,
            });
        }

        // TODO: Create new node

        Ok(FilterPushdownPropagation {
            filters: results,
            updated_node: None,
        })
    }
}

impl DisplayAs for MemQuadPatternExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MemQuadPatternExec: ",)?;

        match &self.planned_scan {
            PlannedPatternScan::Empty(_) => write!(f, "[EMPTY]"),
            PlannedPatternScan::IndexScan {
                index,
                graph_variable,
                pattern,
                ..
            } => {
                write!(f, "[{index}] ")?;

                if let Some(graph_variable) = &graph_variable {
                    write!(f, "graph={graph_variable}, ")?;
                }

                write!(f, "subject={}", &pattern.subject)?;
                write!(f, ", predicate={}", &pattern.predicate)?;
                write!(f, ", object={}", &pattern.object)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::memory::storage::MemQuadPatternExec;
    use crate::memory::{MemObjectIdMapping, MemQuadStorage};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::config::ConfigOptions;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::filter_pushdown::{
        ChildFilterPushdownResult, ChildPushdownResult, FilterPushdownPhase,
        FilterPushdownPropagation, PushedDown,
    };
    use datafusion::physical_plan::{displayable, ExecutionPlan, PhysicalExpr};
    use datafusion::scalar::ScalarValue;
    use insta::assert_snapshot;
    use rdf_fusion_logical::ActiveGraph;
    use rdf_fusion_model::{
        BlankNodeMatchingMode, NamedNode, NamedNodePattern, TermPattern, TriplePattern,
        Variable,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_filter_pushdown_binds_variable() {
        let exec = create_test_pattern().await;
        let filter_expr = create_object_filter_expr(Operator::Eq, 1);
        let result = execute_filter_pushdown(exec, filter_expr);

        assert!(result.filters.iter().all(|f| matches!(f, PushedDown::Yes)));
        assert!(result.updated_node.is_some());
        assert_snapshot!(displayable(result.updated_node.unwrap().as_ref()).indent(false), @r"TODO")
    }

    /// Creates a new [MemQuadPatternExec] for the pattern (?subject <...> ?object) and no graph
    /// variable.
    async fn create_test_pattern() -> MemQuadPatternExec {
        let schema = Arc::new(Schema::new(vec![
            Field::new("subject", DataType::UInt32, false),
            Field::new("object", DataType::UInt32, false),
        ]));
        let pattern = TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked("subject")),
            predicate: NamedNodePattern::NamedNode(NamedNode::new_unchecked(
                "http://example.com/test",
            )),
            object: TermPattern::Variable(Variable::new_unchecked("object")),
        };

        let index = MemQuadStorage::new(Arc::new(MemObjectIdMapping::default()), 10);
        let planned_scan = index
            .snapshot()
            .await
            .plan_pattern_evaluation(
                ActiveGraph::DefaultGraph,
                None,
                pattern,
                BlankNodeMatchingMode::Filter,
            )
            .await
            .unwrap();

        MemQuadPatternExec::new(Arc::new(schema.as_ref().clone()), planned_scan)
    }

    /// Creates a filter operation on the `object` column with the given `operator` and `value`.
    fn create_object_filter_expr(
        operator: Operator,
        value: u32,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("object", 1)),
            operator,
            Arc::new(Literal::new(ScalarValue::UInt32(Some(value)))),
        ))
    }

    /// Runs the filter push down on `exec` with the given `filter_expr`.
    fn execute_filter_pushdown(
        exec: MemQuadPatternExec,
        filter_expr: Arc<dyn PhysicalExpr>,
    ) -> FilterPushdownPropagation<Arc<dyn ExecutionPlan>> {
        let result = exec
            .handle_child_pushdown_result(
                FilterPushdownPhase::Pre,
                ChildPushdownResult {
                    parent_filters: vec![ChildFilterPushdownResult {
                        filter: filter_expr,
                        child_results: vec![],
                    }],
                    self_filters: vec![],
                },
                &ConfigOptions::default(),
            )
            .unwrap();
        result
    }
}
