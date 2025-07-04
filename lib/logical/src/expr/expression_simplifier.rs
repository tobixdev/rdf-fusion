use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{plan_datafusion_err, DFSchema, DFSchemaRef, ExprSchema};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::utils::NamePreserver;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use std::sync::Arc;

/// An optimizer rule that tries to optimize SPARQL expressions.
#[derive(Debug)]
pub struct SimplifySparqlExpressionsRule;

impl SimplifySparqlExpressionsRule {
    /// Creates a new [SimplifySparqlExpressionsRule].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SimplifySparqlExpressionsRule {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for SimplifySparqlExpressionsRule {
    fn name(&self) -> &str {
        "simplify-sparql-expressions"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let schema = if !plan.inputs().is_empty() {
            DFSchemaRef::new(merge_schema(&plan.inputs()))
        } else if let LogicalPlan::TableScan(_) = &plan {
            // There is special handling in DF for this. We just bail out for now.
            return Ok(Transformed::no(plan));
        } else {
            Arc::new(DFSchema::empty())
        };

        // Changing the expression might lead to a name change in the schema.
        let name_preserver = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = name_preserver.save(&expr);
            let expr = try_rewrite_expression(expr, &schema)?;
            Ok(Transformed::new_transformed(
                name.restore(expr.data),
                expr.transformed,
            ))
        })
    }
}

/// Rewrites an [ExtendNode] into a projection.
fn try_rewrite_expression(
    expr: Expr,
    input_schema: &dyn ExprSchema,
) -> DFResult<Transformed<Expr>> {
    expr.transform_up(|expr| match expr {
        Expr::ScalarFunction(scalar_function) => {
            try_rewrite_scalar_function(scalar_function, input_schema)
        }
        _ => Ok(Transformed::no(expr)),
    })
}

fn try_rewrite_scalar_function(
    scalar_function: ScalarFunction,
    input_schema: &dyn ExprSchema,
) -> DFResult<Transformed<Expr>> {
    let function_name = scalar_function.func.name();
    match function_name {
        "IS_COMPATIBLE" => {
            let lhs_nullable = scalar_function.args[0].nullable(input_schema)?;
            let rhs_nullable = scalar_function.args[1].nullable(input_schema)?;

            if !lhs_nullable && !rhs_nullable {
                let [lhs, rhs] =
                    TryInto::<[Expr; 2]>::try_into(scalar_function.args).map_err(|_| {
                        plan_datafusion_err!("Unexpected number of args for IS_COMPATIBLE")
                    })?;
                Ok(Transformed::yes(lhs.eq(rhs)))
            } else {
                Ok(Transformed::no(Expr::ScalarFunction(scalar_function)))
            }
        }
        _ => Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
    }
}
