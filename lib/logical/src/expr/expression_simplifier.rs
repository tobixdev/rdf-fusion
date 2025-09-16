use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef, plan_datafusion_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::utils::NamePreserver;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;
use std::sync::Arc;

/// An optimizer rule that tries to optimize SPARQL expressions.
///
/// Currently, the following transformations are implemented:
/// - IS_COMPATIBLE(A, B) => A = B, if A and B are not nullable
/// - EFFECTIVE_BOOLEAN_VALUE(BOOLEAN_AS_TERM(X)) -> X
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

/// Rewrites the RDF Fusion built-ins in an [Expr].
fn try_rewrite_expression(
    expr: Expr,
    input_schema: &DFSchema,
) -> DFResult<Transformed<Expr>> {
    expr.transform_up(|expr| match expr {
        Expr::ScalarFunction(scalar_function) => {
            try_rewrite_scalar_function(scalar_function, input_schema)
        }
        _ => Ok(Transformed::no(expr)),
    })
}

/// Rewrites the RDF Fusion UDFs.
fn try_rewrite_scalar_function(
    scalar_function: ScalarFunction,
    input_schema: &DFSchema,
) -> DFResult<Transformed<Expr>> {
    let function_name = scalar_function.func.name();
    let builtin = BuiltinName::try_from(function_name);
    let Ok(builtin) = builtin else {
        return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
    };

    match builtin {
        BuiltinName::IsCompatible => {
            try_replace_is_compatible_with_equality(scalar_function, input_schema)
        }
        BuiltinName::EffectiveBooleanValue => {
            try_replace_boolean_round_trip(scalar_function)
        }
        _ => Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
    }
}

/// Replacing `IS_COMPATIBLE` with `=` is a crucial transformation for our queries as DataFusion's
/// built-in optimizers and join algorithms can handle the equality operator.
///
/// # DataFusion Native Approach
///
/// There is also a [ticket](https://github.com/apache/datafusion/issues/15891) that talks about
/// how DataFusion could natively support `IS_COMPATIBLE` semantics. However, this would be a
/// significant investment to actually support it in join algorithms etc.
fn try_replace_is_compatible_with_equality(
    scalar_function: ScalarFunction,
    input_schema: &DFSchema,
) -> DFResult<Transformed<Expr>> {
    let lhs_nullable = scalar_function.args[0].nullable(input_schema)?;
    let rhs_nullable = scalar_function.args[1].nullable(input_schema)?;

    if lhs_nullable || rhs_nullable {
        return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
    }

    let [lhs, rhs] =
        TryInto::<[Expr; 2]>::try_into(scalar_function.args).map_err(|_| {
            plan_datafusion_err!("Unexpected number of args for IS_COMPATIBLE")
        })?;
    Ok(Transformed::yes(lhs.eq(rhs)))
}

/// Tries to replace EBV(BOOLEAN_AS_TERM(X)) with X. This can be a crucial optimization in query
/// plans that use such expressions in filters, as the conversion functions can hinder the optimizer
/// from pushing down parts of filters (e.g., A && B).
fn try_replace_boolean_round_trip(
    scalar_function: ScalarFunction,
) -> DFResult<Transformed<Expr>> {
    let (inner_built_in, args) = match &scalar_function.args[0] {
        Expr::ScalarFunction(inner_function) => {
            let built_in = BuiltinName::try_from(inner_function.func.name());
            let Ok(built_in) = built_in else {
                return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
            };
            (built_in, &inner_function.args)
        }
        _ => return Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
    };

    match inner_built_in {
        BuiltinName::NativeBooleanAsTerm => {
            assert_eq!(
                args.len(),
                1,
                "Unexpected number of args for BOOLEAN_AS_TERM"
            );
            Ok(Transformed::yes(args[0].clone()))
        }
        _ => Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RdfFusionExprBuilderContext;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::{DFSchema, DFSchemaRef};
    use datafusion::logical_expr::{EmptyRelation, LogicalPlan, LogicalPlanBuilder, col};
    use datafusion::optimizer::OptimizerContext;
    use insta::assert_snapshot;
    use rdf_fusion_api::RdfFusionContextView;
    use rdf_fusion_api::functions::{FunctionName, RdfFusionFunctionArgs};
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
    use rdf_fusion_encoding::{
        EncodingName, QuadStorageEncoding, RdfFusionEncodings, TermEncoding,
    };
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;

    #[test]
    fn test_is_compatible_rewrite_when_not_nullable() {
        let schema = make_schema(EncodingName::PlainTerm, false, false);
        let rewritten = execute_test_for_builtin(&schema, BuiltinName::IsCompatible);
        assert_snapshot!(rewritten.data, @r"
        Projection: column1 = column2 AS IS_COMPATIBLE(column1,column2)
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_is_compatible_does_not_rewrite_when_nullable() {
        let schema = make_schema(EncodingName::PlainTerm, false, true);
        let rewritten = execute_test_for_builtin(&schema, BuiltinName::IsCompatible);
        assert_snapshot!(rewritten.data, @r"
        Projection: IS_COMPATIBLE(column1, column2)
          EmptyRelation: rows=0
        ");
    }

    #[test]
    fn test_boolean_round_trip_rewrite() -> DFResult<()> {
        let context = create_context();
        let schema = make_schema(EncodingName::PlainTerm, false, true);
        let expr = RdfFusionExprBuilderContext::new(&context, &schema)
            .try_create_builder(col("column1"))?
            .not()?
            .build_effective_boolean_value()?;

        // Ensure the builder is not optimizing
        assert_eq!(
            expr.to_string(),
            "EBV(BOOLEAN_AS_TERM(NOT EBV(ENC_TV(column1))))"
        );

        let rewritten = execute_test_for_expr(&schema, expr);
        assert_snapshot!(rewritten.data, @r"
        Projection: NOT EBV(ENC_TV(column1)) AS EBV(BOOLEAN_AS_TERM(NOT EBV(ENC_TV(column1))))
          EmptyRelation: rows=0
        ");
        Ok(())
    }

    fn execute_test_for_builtin(
        schema: &DFSchemaRef,
        builtin: BuiltinName,
    ) -> Transformed<LogicalPlan> {
        execute_test_for_builtin_with_args(
            schema,
            builtin,
            vec![col("column1"), col("column2")],
        )
    }

    fn execute_test_for_builtin_with_args(
        schema: &DFSchemaRef,
        builtin: BuiltinName,
        args: Vec<Expr>,
    ) -> Transformed<LogicalPlan> {
        let registry = create_context();
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: registry
                .functions()
                .create_udf(
                    FunctionName::Builtin(builtin),
                    RdfFusionFunctionArgs::empty(),
                )
                .unwrap(),
            args,
        });
        execute_test_for_expr(schema, expr)
    }

    fn execute_test_for_expr(
        schema: &DFSchemaRef,
        expr: Expr,
    ) -> Transformed<LogicalPlan> {
        let plan = create_plan(&schema)
            .project(vec![expr])
            .unwrap()
            .build()
            .unwrap();
        let rule = SimplifySparqlExpressionsRule::new();
        let rewritten = rule.rewrite(plan, &OptimizerContext::new()).unwrap();
        rewritten
    }

    fn create_context() -> RdfFusionContextView {
        let encodings = RdfFusionEncodings::new(
            PLAIN_TERM_ENCODING,
            TYPED_VALUE_ENCODING,
            None,
            SORTABLE_TERM_ENCODING,
        );
        let registry = Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));
        RdfFusionContextView::new(registry, encodings, QuadStorageEncoding::PlainTerm)
    }

    fn make_schema(
        encoding: EncodingName,
        column1_nullable: bool,
        column2_nullable: bool,
    ) -> DFSchemaRef {
        let data_type = match encoding {
            EncodingName::PlainTerm => PLAIN_TERM_ENCODING.data_type(),
            EncodingName::TypedValue => TYPED_VALUE_ENCODING.data_type(),
            _ => panic!("Unsupported encoding"),
        };
        DFSchemaRef::new(
            DFSchema::try_from(Schema::new(vec![
                Field::new("column1", data_type.clone(), column1_nullable),
                Field::new("column2", data_type, column2_nullable),
            ]))
            .unwrap(),
        )
    }

    fn create_plan(schema: &DFSchemaRef) -> LogicalPlanBuilder {
        LogicalPlanBuilder::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::clone(schema),
        }))
    }
}
