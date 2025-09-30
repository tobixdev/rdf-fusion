use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{
    DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner,
};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_physical::paths::KleenePlusPathPlanner;
use std::fmt::Debug;
use std::sync::Arc;

pub struct RdfFusionPlanner {
    /// The RdfFusion context.
    context: RdfFusionContextView,
    /// The storage layer that is used to execute the query.
    storage: Arc<dyn QuadStorage>,
}

impl RdfFusionPlanner {
    /// Creates a new [RdfFusionPlanner].
    pub fn new(context: RdfFusionContextView, storage: Arc<dyn QuadStorage>) -> Self {
        Self { context, storage }
    }
}

impl Debug for RdfFusionPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RdfFusionPlanner")
    }
}

#[async_trait]
impl QueryPlanner for RdfFusionPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(KleenePlusPathPlanner)];
        planners.extend(self.storage.planners(&self.context).await);

        let planner = DefaultPhysicalPlanner::with_extension_planners(planners);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
