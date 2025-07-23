use crate::oxigraph_memory::quad_storage_stream::QuadPatternBatchRecordStream;
use crate::oxigraph_memory::store::MemoryStorageReader;
use crate::MemoryQuadStorage;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::{EmptyRecordBatchStream, ExecutionPlan};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_api::storage::QuadPatternEvaluator;
use rdf_fusion_common::BlankNodeMatchingMode;
use rdf_fusion_encoding::QuadStorageEncoding;
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

    /// Enumerates the graphs in the active graph, expanding wildcards.
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

    /// Enumerates all named graphs in the store.
    fn enumerate_named_graphs(&self) -> DFResult<Vec<GraphName>> {
        self.snapshot
            .named_graphs()
            .map(|et| {
                self.snapshot
                    .object_ids()
                    .try_decode::<GraphName>(et)
                    .map_err(|err| DataFusionError::External(Box::new(err)))
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
    fn schema(&self) -> SchemaRef {
        Arc::clone(QuadStorageEncoding::ObjectId.quad_schema().inner())
    }

    fn evaluate_pattern(
        &self,
        graph: GraphName,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        batch_size: usize,
    ) -> DFResult<SendableRecordBatchStream> {
        let subject = match &pattern.subject {
            TermPattern::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
            TermPattern::BlankNode(bnode) if blank_node_mode == BlankNodeMatchingMode::Filter => {
                Some(Term::BlankNode(bnode.clone()))
            }
            TermPattern::Literal(_) => {
                // If the subject is a literal, then the result is always empty.
                return Ok(empty_result(
                    graph_variable.as_ref(),
                    &pattern,
                    blank_node_mode,
                ));
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

        let Some(graph) = self
            .object_ids()
            .try_get_object_id_for_graph_name(graph.as_ref())
        else {
            // If the there is no matching object id the result is empty.
            return Ok(empty_result(
                graph_variable.as_ref(),
                &pattern,
                blank_node_mode,
            ));
        };
        let subject = match subject {
            None => None,
            Some(subject) => {
                let Some(subject) = self.object_ids().try_get_object_id(subject.as_ref()) else {
                    // If there is no matching object id the result is empty.
                    return Ok(empty_result(
                        graph_variable.as_ref(),
                        &pattern,
                        blank_node_mode,
                    ));
                };
                Some(subject)
            }
        };
        let predicate = match predicate {
            None => None,
            Some(predicate) => {
                let Some(predicate) = self.object_ids().try_get_object_id(predicate.as_ref())
                else {
                    // If there is no matching object id the result is empty.
                    return Ok(empty_result(
                        graph_variable.as_ref(),
                        &pattern,
                        blank_node_mode,
                    ));
                };
                Some(predicate)
            }
        };
        let object = match object {
            None => None,
            Some(object) => {
                let Some(object) = self.object_ids().try_get_object_id(object.as_ref()) else {
                    // If there is no matching object id the result is empty.
                    return Ok(empty_result(
                        graph_variable.as_ref(),
                        &pattern,
                        blank_node_mode,
                    ));
                };
                Some(object)
            }
        };

        let iterator = self.quads_for_pattern(Some(graph), subject, predicate, object);
        Ok(Box::pin(QuadPatternBatchRecordStream::new(
            iterator,
            graph_variable,
            pattern,
            blank_node_mode,
            batch_size,
        )))
    }
}

fn empty_result(
    graph_variable: Option<&Variable>,
    pattern: &TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> SendableRecordBatchStream {
    let schema = compute_schema_for_triple_pattern(
        QuadStorageEncoding::ObjectId,
        graph_variable.as_ref().map(|v| v.as_ref()),
        pattern,
        blank_node_mode,
    );
    Box::pin(EmptyRecordBatchStream::new(Arc::clone(schema.inner())))
}
