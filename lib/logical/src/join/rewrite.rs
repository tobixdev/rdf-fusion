use crate::join::{SparqlJoinNode, SparqlJoinType};
use crate::DFResult;
use crate::GraphFusionExprBuilder;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, JoinType};
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use graphfusion_encoding::EncodingName;
use graphfusion_functions::registry::GraphFusionFunctionRegistryRef;
use std::collections::HashSet;

/// TODO
#[derive(Debug)]
pub struct SparqlJoinLoweringRule {
    /// Used for creating expressions with GraphFusion builtins.
    registry: GraphFusionFunctionRegistryRef,
}

impl OptimizerRule for SparqlJoinLoweringRule {
    fn name(&self) -> &str {
        "sparql-join-lowering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<SparqlJoinNode>() {
                        let result = self.rewrite_sparql_join(node)?;

                        if result.schema() != plan.schema() {
                            panic!(
                                "Schema mismatch!\n result_schema: {:?}\n\nplan_schema: {:?}",
                                result.schema(),
                                plan.schema()
                            );
                        }

                        Transformed::yes(result)
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

impl SparqlJoinLoweringRule {
    /// TODO
    pub fn new(registry: GraphFusionFunctionRegistryRef) -> Self {
        Self { registry }
    }

    /// TODO
    fn rewrite_sparql_join(&self, node: &SparqlJoinNode) -> DFResult<LogicalPlan> {
        let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;
        let filter = node.filter().cloned();

        let lhs_keys: HashSet<_> = lhs
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();
        let rhs_keys: HashSet<_> = rhs
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();

        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());

        let expr_builder = GraphFusionExprBuilder::new(&join_schema, self.registry.as_ref());
        let projections = node
            .schema()
            .columns()
            .into_iter()
            .map(|c| value_from_joined(expr_builder, &lhs_keys, &rhs_keys, c.name()))
            .collect::<DFResult<Vec<_>>>()?;

        // If both solutions are disjoint, use cross join.
        if lhs_keys.is_disjoint(&rhs_keys) && filter.is_none() {
            return lhs.cross_join(rhs.build()?)?.project(projections)?.build();
        }

        let mut join_filters = lhs_keys
            .intersection(&rhs_keys)
            .map(|k| {
                expr_builder.is_compatible(
                    Expr::from(Column::new(Some("lhs"), k)),
                    Expr::from(Column::new(Some("rhs"), k)),
                )
            })
            .collect::<DFResult<Vec<_>>>()?;
        if let Some(filter) = filter {
            let filter = filter
                .transform(|e| {
                    Ok(match e {
                        Expr::Column(c) => Transformed::yes(value_from_joined(
                            expr_builder,
                            &lhs_keys,
                            &rhs_keys,
                            c.name(),
                        )?),
                        _ => Transformed::no(e),
                    })
                })?
                .data;
            let filter = expr_builder.effective_boolean_value(filter)?;
            join_filters.push(filter);
        }
        let filter_expr = join_filters.into_iter().reduce(Expr::and);

        let join_type = match node.join_type() {
            SparqlJoinType::Inner => JoinType::Inner,
            SparqlJoinType::Left => JoinType::Left,
        };
        let join = lhs.join_detailed(
            rhs.build()?,
            join_type,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter_expr,
            false,
        )?;
        join.project(projections)?.build()
    }
}

/// Returns an expression that obtains value `variable` from either the lhs, the rhs, or both
/// depending on the schema.
fn value_from_joined(
    expr_builder: GraphFusionExprBuilder<'_>,
    lhs_keys: &HashSet<String>,
    rhs_keys: &HashSet<String>,
    variable: &str,
) -> DFResult<Expr> {
    let lhs_expr = Expr::from(Column::new(Some("lhs"), variable));
    let rhs_expr = Expr::from(Column::new(Some("rhs"), variable));

    let expr = match (lhs_keys.contains(variable), rhs_keys.contains(variable)) {
        (true, true) => {
            let coalesce = expr_builder.coalesce(vec![lhs_expr, rhs_expr])?;
            expr_builder.with_encoding(coalesce, EncodingName::PlainTerm)?
        }
        (true, false) => lhs_expr,
        (false, true) => rhs_expr,
        (false, false) => unreachable!("At least one of lhs or rhs must contain variable"),
    };
    Ok(expr.alias(variable))
}
