use crate::extend::ExtendNode;
use crate::quads::QuadsNode;
use crate::{DFResult, GraphFusionExprBuilder, GraphFusionLogicalPlanBuilder};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{
    col, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use graphfusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use graphfusion_functions::registry::GraphFusionFunctionRegistryRef;
use graphfusion_model::TermRef;
use std::sync::Arc;

/// TODO
#[derive(Debug)]
pub struct ExtendLoweringRule {
    /// Used for creating expressions with GraphFusion builtins.
    registry: GraphFusionFunctionRegistryRef,
    /// Reference to the registered Quads Table.
    quads_table: Arc<dyn TableProvider>,
}

impl OptimizerRule for ExtendLoweringRule {
    fn name(&self) -> &str {
        "extend-lowering"
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
                        Transformed::yes(self.rewrite_extend_node(node)?)
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
    pub fn new(
        registry: GraphFusionFunctionRegistryRef,
        quads_table: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            registry,
            quads_table,
        }
    }

    /// TODO
    fn rewrite_extend_node(&self, node: &ExtendNode) -> DFResult<LogicalPlan> {
        let mut new_exprs: Vec<_> = node
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
