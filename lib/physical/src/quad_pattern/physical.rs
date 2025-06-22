use datafusion::common::{internal_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use rdf_fusion_common::{DFResult, QuadPatternEvaluator};
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_DFSCHEMA;
use rdf_fusion_logical::patterns::compute_schema_for_pattern;
use rdf_fusion_logical::EnumeratedActiveGraph;
use rdf_fusion_model::{TriplePattern, Variable};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// TODO
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
    /// The execution properties of this operator.
    plan_properties: PlanProperties,
}

impl QuadPatternExec {
    /// TODO
    pub fn new(
        quads_evaluator: Arc<dyn QuadPatternEvaluator>,
        active_graph: EnumeratedActiveGraph,
        graph_variable: Option<Variable>,
        triple_pattern: TriplePattern,
    ) -> Self {
        let schema = Arc::clone(
            compute_schema_for_pattern(
                DEFAULT_QUAD_DFSCHEMA.as_ref(),
                &[
                    graph_variable.as_ref().map(|v| v.clone().into()),
                    Some(triple_pattern.subject.clone()),
                    Some(triple_pattern.predicate.clone().into()),
                    Some(triple_pattern.object.clone()),
                ],
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
            plan_properties,
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

        self.quads_evaluator.evaluate_pattern(
            self.active_graph.0[partition].clone(),
            self.graph_variable.clone(),
            self.triple_pattern.clone(),
            context.session_config().batch_size(),
        )
    }
}

impl DisplayAs for QuadPatternExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QuadPatternExec ({} Graphs, ",
            self.plan_properties.partitioning.partition_count()
        )?;

        if let Some(graph_variable) = &self.graph_variable {
            write!(f, " {graph_variable}")?;
        }

        write!(f, " {}", &self.triple_pattern)
    }
}
