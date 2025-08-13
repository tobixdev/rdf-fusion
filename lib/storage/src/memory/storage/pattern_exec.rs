use crate::memory::storage::MemQuadStorageSnapshot;
use datafusion::common::{exec_err, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{
    Boundedness, EmissionType, SchedulingType,
};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_model::{TriplePattern, Variable};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemQuadPatternExec {
    /// Access to the storage
    snapshot: MemQuadStorageSnapshot,
    /// The active graph.
    active_graph: ActiveGraph,
    /// Whether to bind the graph to the variable.
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

impl MemQuadPatternExec {
    /// Creates a new [MemQuadPatternExec].
    pub fn new(
        snapshot: MemQuadStorageSnapshot,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        triple_pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> Self {
        let schema = Arc::clone(
            compute_schema_for_triple_pattern(
                snapshot.storage_encoding(),
                graph_variable.as_ref().map(|v| v.as_ref()),
                &triple_pattern,
                blank_node_mode,
            )
            .inner(),
        );
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative); // We wrap our stream in `cooperative`

        Self {
            snapshot,
            active_graph,
            graph_variable,
            triple_pattern,
            blank_node_mode,
            plan_properties,
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
        let result = self.snapshot.evaluate_pattern(
            self.active_graph.clone(),
            self.graph_variable.clone(),
            self.triple_pattern.clone(),
            self.blank_node_mode,
            baseline_metrics,
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

impl DisplayAs for MemQuadPatternExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MemQuadPatternExec: ",)?;

        write!(f, ", active_graph={}", &self.active_graph)?;
        if let Some(graph_variable) = &self.graph_variable {
            write!(f, ", graph={graph_variable}")?;
        }

        write!(f, ", subject={}", &self.triple_pattern.subject)?;
        write!(f, ", predicate={}", &self.triple_pattern.predicate)?;
        write!(f, ", object={}", &self.triple_pattern.object)
    }
}
