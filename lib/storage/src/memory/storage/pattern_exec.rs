use crate::memory::storage::index::PlannedPatternScan;
use datafusion::arrow::datatypes::SchemaRef;
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
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug)]
pub struct MemQuadPatternExec {
    /// The execution properties of this operator.
    plan_properties: PlanProperties,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// TODO
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
