use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use rdf_fusion_logical::{ActiveGraph, EnumeratedActiveGraph};
use rdf_fusion_model::GraphName;
use std::sync::Arc;

/// Planner for [QuadPatternNode].
pub struct MemQuadStorePlanner;

impl MemQuadStorePlanner {
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
        todo!("Implement named graph enumeration");
    }
}

#[async_trait]
impl ExtensionPlanner for MemQuadStorePlanner {
    /// Converts a logical [QuadPatternNode] into its physical execution plan
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(_) = node.as_any().downcast_ref::<QuadPatternNode>() {
            todo!("Implement quad pattern planning");
        } else {
            Ok(None)
        }
    }
}
