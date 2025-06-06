use async_trait::async_trait;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use rdf_fusion_common::QuadPatternEvaluator;
use rdf_fusion_functions::registry::RdfFusionFunctionRegistry;
use rdf_fusion_physical::paths::KleenePlusPathPlanner;
use rdf_fusion_physical::quads::QuadNodePlanner;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Default)]
pub struct RdfFusionPlanner {
    inner_planner: DefaultPhysicalPlanner,
}

impl RdfFusionPlanner {
    /// TODO
    pub fn new(
        function_registry: Arc<dyn RdfFusionFunctionRegistry>,
        quad_pattern_evaluator: Arc<dyn QuadPatternEvaluator>,
    ) -> Self {
        let inner_planner = DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(KleenePlusPathPlanner),
            Arc::new(QuadNodePlanner::new(
                function_registry,
                quad_pattern_evaluator,
            )),
        ]);
        Self { inner_planner }
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
        self.inner_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
