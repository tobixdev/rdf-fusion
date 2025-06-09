use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
#[allow(clippy::struct_field_names)]
pub struct QueryExplanation {
    /// The initial logical plan created from the SPARQL query.
    pub initial_logical_plan: LogicalPlan,
    /// The optimized logical plan.
    pub optimized_logical_plan: LogicalPlan,
    /// A reference to the root node of the plan that was actually executed.
    pub executed_plan: Arc<dyn ExecutionPlan>,
}
