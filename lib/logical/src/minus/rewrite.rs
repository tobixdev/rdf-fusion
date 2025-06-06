use crate::check_same_schema;
use crate::minus::MinusNode;
use crate::RdfFusionExprBuilderRoot;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{plan_datafusion_err, Column, DFSchemaRef, JoinType};
use datafusion::logical_expr::{and, Expr, UserDefinedLogicalNode};
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_functions::registry::RdfFusionFunctionRegistryRef;
use std::collections::HashSet;
use std::sync::Arc;
use rdf_fusion_common::DFResult;

/// TODO
#[derive(Debug)]
pub struct MinusLoweringRule {
    /// Holds a reference to the RdfFusion function registry.
    registry: RdfFusionFunctionRegistryRef,
}

impl OptimizerRule for MinusLoweringRule {
    fn name(&self) -> &str {
        "minus-lowering"
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
                    if let Some(node) = node.as_any().downcast_ref::<MinusNode>() {
                        let new_plan = self.rewrite_minus(node)?;
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

impl MinusLoweringRule {
    /// TODO
    pub fn new(registry: RdfFusionFunctionRegistryRef) -> Self {
        Self { registry }
    }

    /// TODO
    fn rewrite_minus(&self, node: &MinusNode) -> DFResult<LogicalPlan> {
        let overlapping_keys = compute_join_keys(node);

        // If there are no overlapping keys, then we cannot remove any solution.
        if overlapping_keys.is_empty() {
            return Ok(node.lhs().clone());
        }

        let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;
        let lhs_schema = Arc::clone(lhs.schema());

        // Compute the result via a LeftAnti join.
        let filter_expr =
            self.compute_filter_expression(lhs.schema(), rhs.schema(), &overlapping_keys)?;
        let join_result = lhs.join_detailed(
            rhs.build()?,
            JoinType::LeftAnti,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter_expr,
            false,
        )?;

        // Eliminate the "lhs" qualifier.
        let projections = lhs_schema
            .columns()
            .into_iter()
            .map(|k| Expr::from(Column::new(Some("lhs"), &k.name)).alias(k.name))
            .collect::<Vec<_>>();
        join_result.project(projections)?.build()
    }

    /// TODO
    fn compute_filter_expression(
        &self,
        lhs_schema: &DFSchemaRef,
        rhs_schema: &DFSchemaRef,
        overlapping_keys: &HashSet<String>,
    ) -> DFResult<Option<Expr>> {
        let mut join_schema = lhs_schema.as_ref().clone();
        join_schema.merge(rhs_schema);
        let expr_builder_root = RdfFusionExprBuilderRoot::new(self.registry.as_ref(), &join_schema);

        let mut join_filters = Vec::new();

        // Filter based on the overlapping keys.
        for k in overlapping_keys {
            let expr = expr_builder_root
                .try_create_builder(Expr::from(Column::new(Some("lhs"), k)))?
                .build_is_compatible(Expr::from(Column::new(Some("rhs"), k)))?;
            join_filters.push(expr);
        }

        // At least one of the overlapping keys must be not null.
        let any_both_not_null = overlapping_keys
            .iter()
            .map(|k| {
                and(
                    Expr::from(Column::new(Some("lhs"), k)).is_not_null(),
                    Expr::from(Column::new(Some("rhs"), k)).is_not_null(),
                )
            })
            .reduce(Expr::or)
            .ok_or(plan_datafusion_err!(
                "There must be at least one overlapping key"
            ))?;
        join_filters.push(any_both_not_null);

        let filter_expr = join_filters.into_iter().reduce(Expr::and);
        Ok(filter_expr)
    }
}

/// Computes the overlapping columns between the lhs and rhs of the minus.
fn compute_join_keys(node: &MinusNode) -> HashSet<String> {
    let lhs_keys: HashSet<_> = node
        .lhs()
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();
    let rhs_keys: HashSet<_> = node
        .rhs()
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();

    let overlapping_keys = lhs_keys
        .intersection(&rhs_keys)
        .cloned()
        .collect::<HashSet<String>>();

    overlapping_keys
}
