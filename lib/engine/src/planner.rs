use async_trait::async_trait;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use rdf_fusion_physical::paths::KleenePlusPathPlanner;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct RdfFusionPlanner;

#[async_trait]
impl QueryPlanner for RdfFusionPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(KleenePlusPathPlanner)]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
