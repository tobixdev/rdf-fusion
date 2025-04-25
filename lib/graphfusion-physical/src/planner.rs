use crate::paths::KleenePlusPathPlanner;
use async_trait::async_trait;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct GraphFusionPlanner;

#[async_trait]
impl QueryPlanner for GraphFusionPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let physical_planner = DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
            KleenePlusPathPlanner::new(),
        )]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
