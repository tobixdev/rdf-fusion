use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::quad_storage_stream::QuadIteratorBatchRecordStream;
use crate::oxigraph_memory::store::MemoryStorageReader;
use crate::MemoryQuadStorage;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::QuadPatternEvaluator;
use rdf_fusion_logical::quads::QuadsNode;
use rdf_fusion_logical::{ActiveGraph, EnumeratedActiveGraph};
use rdf_fusion_model::{GraphName, GraphNameRef, NamedNodeRef, SubjectRef, TermRef};
use rdf_fusion_physical::quads::QuadsExec;
use std::sync::Arc;

/// Planner for [QuadsNode].
pub struct OxigraphMemoryQuadNodePlanner {
    /// The implementation of the quad pattern evaluator.
    snapshot: MemoryStorageReader,
}

impl OxigraphMemoryQuadNodePlanner {
    /// Creates a new [OxigraphMemoryQuadNodePlanner].
    pub fn new(storage: MemoryQuadStorage) -> Self {
        Self {
            snapshot: storage.snapshot(),
        }
    }

    /// TODO
    fn enumerate_active_graph(&self, active_graph: &ActiveGraph) -> DFResult<EnumeratedActiveGraph> {
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
    /// Converts a logical [QuadsNode] into its physical execution plan
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<QuadsNode>() {
            let active_graph = self.enumerate_active_graph(node.active_graph())?;
            let quads = Arc::new(QuadsExec::new(
                Arc::new(self.snapshot.clone()),
                active_graph,
                node.subject().cloned(),
                node.predicate().cloned(),
                node.object().cloned(),
            ));
            Ok(Some(quads))
        } else {
            Ok(None)
        }
    }
}
#[async_trait]
impl QuadPatternEvaluator for MemoryStorageReader {
    fn quads_for_pattern(
        &self,
        graph_name: GraphNameRef<'_>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
        batch_size: usize,
    ) -> rdf_fusion_common::DFResult<SendableRecordBatchStream> {
        let iterator = self.quads_for_pattern(
            subject.map(EncodedTerm::from).as_ref(),
            predicate.map(EncodedTerm::from).as_ref(),
            object.map(EncodedTerm::from).as_ref(),
            Some(&EncodedTerm::from(graph_name)),
        );
        Ok(Box::pin(QuadIteratorBatchRecordStream::new(
            iterator, batch_size,
        )))
    }
}
