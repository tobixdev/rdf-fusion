use crate::MemoryQuadStorage;
use crate::oxigraph_memory::encoded::EncodedTerm;
use crate::oxigraph_memory::quad_exec::MemoryQuadExec;
use crate::oxigraph_memory::store::MemoryStorageReader;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::error::{CorruptionError, StorageError};
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use rdf_fusion_logical::{ActiveGraph, EnumeratedActiveGraph};
use rdf_fusion_model::{BlankNode, GraphName, NamedNode, VariableRef};
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
        let object_id_mapping = self.snapshot.object_ids();
        self.snapshot
            .named_graphs()
            .map(|et| {
                let object_id = object_id_mapping
                    .try_get_encoded_term_from_object_id(et)
                    .ok_or_else(|| DataFusionError::External(Box::new(StorageError::Corruption(CorruptionError::new("Encountered unknown object id when enumerating named graphs")))))?;

                 match object_id {
                        EncodedTerm::NamedNode(nn) => Ok(GraphName::NamedNode(
                            NamedNode::new_unchecked(nn.as_ref()),
                        )),
                        EncodedTerm::BlankNode(nn) => Ok(GraphName::BlankNode(
                            BlankNode::new_unchecked(nn.as_ref()),
                        )),
                        _ => Err(DataFusionError::External(Box::new(StorageError::Corruption(CorruptionError::new(
                            "Got literal for named graph.",
                        ))))),
                    }
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
            let active_graph =
                self.enumerate_active_graph(quad_pattern_node.active_graph())?;
            let quads = Arc::new(MemoryQuadExec::new(
                self.snapshot.clone(),
                active_graph,
                quad_pattern_node
                    .graph_variable()
                    .map(VariableRef::into_owned),
                quad_pattern_node.pattern().clone(),
                quad_pattern_node.blank_node_mode(),
            ));

            if node.schema().inner().as_ref() != quads.schema().as_ref() {
                return plan_err!(
                    "Schema does not match after planning QuadPatternExec."
                );
            }

            Ok(Some(quads))
        } else {
            Ok(None)
        }
    }
}
