use crate::oxigraph_memory::store::MemoryStorageReader;
use datafusion::common::stats::Precision;
use datafusion::common::{
    ColumnStatistics, Statistics, exec_err, internal_err, plan_err,
};
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
use rdf_fusion_logical::EnumeratedActiveGraph;
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_model::{GraphName, TriplePattern, Variable};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// Physical execution plan for matching quad patterns.
///
/// This operator is responsible for scanning the underlying storage for quads that match
/// a given pattern. It can produce multiple partitions by partitioning on the active graph.
///
/// Storage layers are expected to provide a custom planner that provides a custom quads_evaluator.
#[derive(Debug, Clone)]
pub struct MemoryQuadExec {
    /// Access to the storage
    memory_storage_reader: MemoryStorageReader,
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

impl MemoryQuadExec {
    /// Creates a new [MemoryQuadExec].
    pub fn new(
        memory_storage_reader: MemoryStorageReader,
        active_graph: EnumeratedActiveGraph,
        graph_variable: Option<Variable>,
        triple_pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> Self {
        let schema = Arc::clone(
            compute_schema_for_triple_pattern(
                &memory_storage_reader.storage().storage_encoding(),
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
        )
        .with_scheduling_type(SchedulingType::Cooperative); // We wrap our stream in `cooperative`

        Self {
            memory_storage_reader,
            active_graph,
            graph_variable,
            triple_pattern,
            blank_node_mode,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }

    fn estimate_num_rows(&self, graph: &GraphName) -> usize {
        self.memory_storage_reader
            .estimate_num_rows(graph, &self.triple_pattern)
    }
}

impl ExecutionPlan for MemoryQuadExec {
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
        let result = self.memory_storage_reader.evaluate_pattern(
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

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Statistics> {
        let num_rows = match partition {
            None => {
                let mut total_rows = 0;
                for i in 0..self.active_graph.0.len() {
                    total_rows += self.estimate_num_rows(&self.active_graph.0[i]);
                }
                total_rows
            }
            Some(partition) => {
                if partition >= self.active_graph.0.len() {
                    // This operator requires a single partition as input.
                    return internal_err!(
                        "Partition index {partition} is out of range. Number of partitions: {}",
                        self.active_graph.0.len()
                    );
                }

                self.estimate_num_rows(&self.active_graph.0[partition])
            }
        };

        Ok(Statistics {
            num_rows: Precision::Inexact(num_rows),
            total_byte_size: Precision::Inexact(num_rows * size_of::<u32>() * 4),
            column_statistics: vec![ColumnStatistics::new_unknown(); 4],
        })
    }
}

impl DisplayAs for MemoryQuadExec {
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
