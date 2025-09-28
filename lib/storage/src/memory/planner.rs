use crate::memory::storage::{MemQuadPatternExec, MemQuadStorageSnapshot};
use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

/// Plans [QuadPatternNode]s with a [MemQuadStore].
pub struct MemQuadStorePlanner {
    /// The snapshot to use for planning.
    snapshot: MemQuadStorageSnapshot,
}

impl MemQuadStorePlanner {
    /// Creates a new [MemQuadStorePlanner] for the given snapshot.
    pub fn new(snapshot: MemQuadStorageSnapshot) -> Self {
        Self { snapshot }
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
        if let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() {
            let plan = self
                .snapshot
                .plan_pattern_evaluation(
                    node.active_graph().clone(),
                    node.graph_variable().map(|g| g.into_owned()),
                    node.pattern().clone(),
                    node.blank_node_mode(),
                )
                .await?;
            let schema = Arc::clone(node.schema().inner());

            if plan.is_guaranteed_empty() {
                return Ok(Some(Arc::new(EmptyExec::new(schema))));
            }

            Ok(Some(Arc::new(MemQuadPatternExec::new(schema, plan))))
        } else {
            Ok(None)
        }
    }
}
