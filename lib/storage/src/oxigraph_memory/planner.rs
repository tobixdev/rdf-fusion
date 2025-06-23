use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::quad_storage_stream::QuadPatternBatchRecordStream;
use crate::oxigraph_memory::store::MemoryStorageReader;
use crate::MemoryQuadStorage;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{EmptyRecordBatchStream, ExecutionPlan};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::{BlankNodeMatchingMode, QuadPatternEvaluator};
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use rdf_fusion_logical::{ActiveGraph, EnumeratedActiveGraph};
use rdf_fusion_model::{
    GraphName, NamedNodePattern, Term, TermPattern, TriplePattern, Variable, VariableRef,
};
use rdf_fusion_physical::quad_pattern::QuadPatternExec;
use std::sync::Arc;

/// Planner for [QuadPatternNode].
pub struct OxigraphMemoryQuadNodePlanner {
    /// The implementation of the quad pattern evaluator.
    snapshot: MemoryStorageReader,
}

impl OxigraphMemoryQuadNodePlanner {
    /// Creates a new [OxigraphMemoryQuadNodePlanner].
    pub fn new(storage: &MemoryQuadStorage) -> Self {
        Self {
            snapshot: storage.snapshot(),
        }
    }

    /// TODO
    fn enumerate_active_graph(
        &self,
        active_graph: &ActiveGraph,
    ) -> DFResult<EnumeratedActiveGraph> {
        let graph_names = match active_graph {
            ActiveGraph::DefaultGraph => vec![GraphName::DefaultGraph],
            ActiveGraph::AllGraphs => {
                let mut all_named_graphs = self.enumerate_named_graphs()?;
                all_named_graphs.push(GraphName::DefaultGraph);
                all_named_graphs
            }
            ActiveGraph::Union(vec) => vec.clone(),
            ActiveGraph::AnyNamedGraph => self.enumerate_named_graphs()?,
        };
        Ok(EnumeratedActiveGraph::new(graph_names))
    }

    /// TODO
    fn enumerate_named_graphs(&self) -> DFResult<Vec<GraphName>> {
        self.snapshot
            .named_graphs()
            .map(|et| match et {
                EncodedTerm::DefaultGraph => Ok(GraphName::DefaultGraph),
                EncodedTerm::NamedNode(nn) => Ok(GraphName::NamedNode(nn)),
                EncodedTerm::BlankNode(bnode) => Ok(GraphName::BlankNode(bnode)),
                EncodedTerm::Literal(_) => plan_err!("Literal found for NamedGraph."),
            })
            .collect::<DFResult<Vec<_>>>()
    }
}

#[async_trait]
impl ExtensionPlanner for OxigraphMemoryQuadNodePlanner {
    /// Converts a logical [QuadPatternNode] into its physical execution plan
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(quad_pattern_node) = node.as_any().downcast_ref::<QuadPatternNode>() {
            let active_graph = self.enumerate_active_graph(quad_pattern_node.active_graph())?;
            let quads = Arc::new(QuadPatternExec::new(
                Arc::new(self.snapshot.clone()),
                active_graph,
                quad_pattern_node
                    .graph_variable()
                    .map(VariableRef::into_owned),
                quad_pattern_node.pattern().clone(),
                quad_pattern_node.blank_node_mode(),
            ));

            if node.schema().inner().as_ref() != quads.schema().as_ref() {
                return plan_err!("Schema does not match after planning QuadPatternExec.");
            }

            Ok(Some(quads))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl QuadPatternEvaluator for MemoryStorageReader {
    fn evaluate_pattern(
        &self,
        graph: GraphName,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        batch_size: usize,
    ) -> rdf_fusion_common::DFResult<SendableRecordBatchStream> {
        let subject = match &pattern.subject {
            TermPattern::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
            TermPattern::BlankNode(bnode) if blank_node_mode == BlankNodeMatchingMode::Filter => {
                Some(Term::BlankNode(bnode.clone()))
            }
            TermPattern::Literal(_) => {
                // If the subject is a literal, then the result is always empty.
                let schema = compute_schema_for_triple_pattern(
                    graph_variable.as_ref().map(|v| v.as_ref()),
                    &pattern,
                    blank_node_mode,
                );
                return Ok(Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                    schema.inner(),
                ))));
            }
            _ => None,
        };
        let predicate = match &pattern.predicate {
            NamedNodePattern::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
            NamedNodePattern::Variable(_) => None,
        };
        let object = match &pattern.object {
            TermPattern::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
            TermPattern::BlankNode(bnode) if blank_node_mode == BlankNodeMatchingMode::Filter => {
                Some(Term::BlankNode(bnode.clone()))
            }
            TermPattern::Literal(lit) => Some(Term::Literal(lit.clone())),
            _ => None,
        };

        let iterator = self.quads_for_pattern(
            Some(&EncodedTerm::from(graph.as_ref())),
            subject.as_ref().map(|t| t.as_ref().into()).as_ref(),
            predicate
                .as_ref()
                .map(|t| t.as_ref())
                .map(EncodedTerm::from)
                .as_ref(),
            object
                .as_ref()
                .map(|t| t.as_ref())
                .map(EncodedTerm::from)
                .as_ref(),
        );
        Ok(Box::pin(QuadPatternBatchRecordStream::new(
            iterator,
            graph_variable,
            pattern,
            blank_node_mode,
            batch_size,
        )))
    }
}
