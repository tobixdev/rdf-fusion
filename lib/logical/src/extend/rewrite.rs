use crate::extend::ExtendNode;
use crate::{check_same_schema, DFResult};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::logical_expr::{
    col, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// TODO
#[derive(Debug)]
pub struct ExtendLoweringRule {}

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
                        let new_plan = self.rewrite_extend_node(node)?;
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

impl ExtendLoweringRule {
    /// TODO
    pub fn new() -> Self {
        Self {}
    }

    /// TODO
    fn rewrite_extend_node(&self, node: &ExtendNode) -> DFResult<LogicalPlan> {
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
}
