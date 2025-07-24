use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct QueryExplanation {
    /// The time spent planning the query.
    pub planning_time: std::time::Duration,
    /// The initial logical plan created from the SPARQL query.
    pub initial_logical_plan: LogicalPlan,
    /// The optimized logical plan.
    pub optimized_logical_plan: LogicalPlan,
    /// A reference to the root node of the plan that was actually executed.
    pub execution_plan: Arc<dyn ExecutionPlan>,
}
