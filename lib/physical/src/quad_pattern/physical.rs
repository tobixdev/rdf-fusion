use datafusion::common::{exec_err, internal_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use rdf_fusion_api::storage::QuadPatternEvaluator;
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_logical::EnumeratedActiveGraph;
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_model::{TriplePattern, Variable};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Physical execution plan for matching quad patterns.
///
/// This operator is responsible for scanning the underlying storage for quads that match
/// a given pattern. It can produce multiple partitions by partitioning on the active graph.
///
/// Storage layers are expected to provide a custom planner that provides a custom quads_evaluator.
#[derive(Debug, Clone)]
pub struct QuadPatternExec {
    /// The actual implementation of the storage layer.
    quads_evaluator: Arc<dyn QuadPatternEvaluator>,
    /// Contains a list of graph names. Each [GraphName] corresponds to a partition.
    active_graph: EnumeratedActiveGraph,
    /// The graph variable.
    graph_variable: Option<Variable>,
    /// The triple pattern to match.
    triple_pattern: TriplePattern,
    /// How to interpret blank nodes.
    blank_node_mode: BlankNodeMatchingMode,
    /// The execution properties of this operator.
    plan_properties: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl QuadPatternExec {
    /// Creates a new [QuadPatternExec].
    pub fn new(
        quads_evaluator: Arc<dyn QuadPatternEvaluator>,
        active_graph: EnumeratedActiveGraph,
        graph_variable: Option<Variable>,
        triple_pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> Self {
        let schema = Arc::clone(
            compute_schema_for_triple_pattern(
                &quads_evaluator.storage_encoding(),
                graph_variable.as_ref().map(|v| v.as_ref()),
                &triple_pattern,
                blank_node_mode,
            )
            .inner(),
        );
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(active_graph.0.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            quads_evaluator,
            active_graph,
            graph_variable,
            triple_pattern,
            blank_node_mode,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }
}

impl ExecutionPlan for QuadPatternExec {
    fn name(&self) -> &str {
        "QuadPatternExec"
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
        if !children.is_empty() {
            return plan_err!("QuadPatternExec has no child, got {}", children.len());
        }
        Ok(Arc::new((*self).clone()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.active_graph.0.len() {
            // This operator requires a single partition as input.
            return internal_err!(
                "Partition index {partition} is out of range. Number of partitions: {}",
                self.active_graph.0.len()
            );
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let result = self.quads_evaluator.evaluate_pattern(
            self.active_graph.0[partition].clone(),
            self.graph_variable.clone(),
            self.triple_pattern.clone(),
            self.blank_node_mode,
            baseline_metrics,
            context.session_config().batch_size(),
        )?;
        if result.schema() != self.schema() {
            return exec_err!("Unexpected schema for quad pattern stream.");
        }

        Ok(result)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for QuadPatternExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QuadPatternExec: n_graphs={}",
            self.plan_properties.partitioning.partition_count()
        )?;

        if let Some(graph_variable) = &self.graph_variable {
            write!(f, ", graph={graph_variable}")?;
        }

        write!(f, ", subject={}", &self.triple_pattern.subject)?;
        write!(f, ", predicate={}", &self.triple_pattern.predicate)?;
        write!(f, ", object={}", &self.triple_pattern.object)
    }
}
