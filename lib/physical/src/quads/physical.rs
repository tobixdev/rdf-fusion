use datafusion::common::{internal_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use rdf_fusion_common::{DFResult, QuadPatternEvaluator};
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_SCHEMA;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_model::{GraphName, NamedNode, Subject, Term};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// TODO
#[derive(Debug, Clone)]
pub struct QuadsExec {
    /// The actual implementation of the storage layer.
    quads_evaluator: Arc<dyn QuadPatternEvaluator>,
    /// Contains a mapping of which graph is emitted in which partition.
    partitions: Vec<Option<GraphName>>,
    /// The execution properties of this operator.
    plan_properties: PlanProperties,
    subject: Option<Subject>,
    predicate: Option<NamedNode>,
    object: Option<Term>,
}

impl QuadsExec {
    /// TODO
    pub fn new(
        quads_evaluator: Arc<dyn QuadPatternEvaluator>,
        active_graph: ActiveGraph,
        subject: Option<Subject>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
    ) -> Self {
        let partitions = match active_graph {
            ActiveGraph::DefaultGraph => vec![Some(GraphName::DefaultGraph)],
            ActiveGraph::AllGraphs => vec![None],
            ActiveGraph::Union(graphs) => graphs.iter().map(|g| Some(g.clone())).collect(),
            ActiveGraph::AnyNamedGraph => vec![None],
        };

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(DEFAULT_QUAD_SCHEMA.clone()),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            quads_evaluator,
            partitions,
            plan_properties,
            subject,
            predicate,
            object,
        }
    }
}

impl ExecutionPlan for QuadsExec {
    fn name(&self) -> &str {
        "QuadsExec"
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
        if children.len() != 0 {
            return plan_err!("QuadsExec has no child, got {}", children.len());
        }
        Ok(Arc::new((*self).clone()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.partitions.len() {
            // This operator requires a single partition as input.
            return internal_err!(
                "Partition index {partition} is out of range. Number of partitions: {}",
                self.partitions.len()
            );
        }

        let graph_name = self.partitions[partition].as_ref();
        self.quads_evaluator.quads_for_pattern(
            self.subject.as_ref(),
            self.predicate.as_ref(),
            self.object.as_ref(),
            graph_name,
            context.session_config().batch_size(),
        )
    }
}

impl DisplayAs for QuadsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QuadsExec ({} Graphs)",
            self.plan_properties.partitioning.partition_count()
        )?;

        if let Some(subject) = &self.subject {
            write!(f, " subject={subject}")?;
        }

        if let Some(predicate) = &self.predicate {
            write!(f, " predicate={predicate}")?;
        }

        if let Some(object) = &self.object {
            write!(f, " object={object}")?;
        }

        Ok(())
    }
}
