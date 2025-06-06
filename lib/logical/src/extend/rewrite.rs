use crate::check_same_schema;
use crate::extend::ExtendNode;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::logical_expr::{
    col, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;

/// TODO
#[derive(Debug)]
pub struct ExtendLoweringRule;

impl ExtendLoweringRule {
    /// TODO
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ExtendLoweringRule {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for ExtendLoweringRule {
    fn name(&self) -> &str {
        "extend-lowering"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<ExtendNode>() {
                        let new_plan = rewrite_extend_node(node)?;
                        check_same_schema(node.schema(), new_plan.schema())?;
                        Transformed::yes(new_plan)
                    } else {
                        Transformed::no(plan)
                    }
                }
                _ => Transformed::no(plan),
            };
            Ok(new_plan)
        })
    }
}

/// TODO
fn rewrite_extend_node(node: &ExtendNode) -> DFResult<LogicalPlan> {
    let mut new_exprs: Vec<_> = node
        .inner()
        .schema()
        .fields()
        .iter()
        .map(|f| col(Column::new_unqualified(f.name())))
        .collect();
    new_exprs.push(node.expression().clone().alias(node.variable().as_str()));

    LogicalPlanBuilder::new(node.inner().clone())
        .project(new_exprs)?
        .build()
}
