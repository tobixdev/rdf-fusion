use crate::check_same_schema;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, DFSchemaRef, ExprSchema, plan_datafusion_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::utils::NamePreserver;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_api::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionArgs, RdfFusionFunctionRegistry,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::{PLAIN_TERM_ENCODING, PlainTermType};
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use std::sync::Arc;

/// An optimizer rule that tries to optimize SPARQL expressions.
#[derive(Debug)]
pub struct SimplifySparqlExpressionsRule {
    /// Used for constructing new expressions.
    registry: Arc<dyn RdfFusionFunctionRegistry>,
}

impl SimplifySparqlExpressionsRule {
    /// Creates a new [SimplifySparqlExpressionsRule].
    pub fn new(registry: Arc<dyn RdfFusionFunctionRegistry>) -> Self {
        Self { registry }
    }

    /// Rewrites an [ExtendNode] into a projection.
    fn try_rewrite_expression(
        &self,
        expr: Expr,
        input_schema: &dyn ExprSchema,
    ) -> DFResult<Transformed<Expr>> {
        expr.transform_up(|expr| match expr {
            Expr::ScalarFunction(scalar_function) => {
                self.try_rewrite_scalar_function(scalar_function, input_schema)
            }
            _ => Ok(Transformed::no(expr)),
        })
    }

    fn try_rewrite_scalar_function(
        &self,
        scalar_function: ScalarFunction,
        input_schema: &dyn ExprSchema,
    ) -> DFResult<Transformed<Expr>> {
        let function_name = scalar_function.func.name();
        let Ok(built_in) = BuiltinName::try_from(function_name) else {
            return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
        };

        match built_in {
            BuiltinName::IsCompatible => {
                let lhs_nullable = scalar_function.args[0].nullable(input_schema)?;
                let rhs_nullable = scalar_function.args[1].nullable(input_schema)?;

                if !lhs_nullable && !rhs_nullable {
                    let [lhs, rhs] = TryInto::<[Expr; 2]>::try_into(scalar_function.args)
                        .map_err(|_| {
                            plan_datafusion_err!(
                                "Unexpected number of args for IS_COMPATIBLE"
                            )
                        })?;
                    Ok(Transformed::yes(lhs.eq(rhs)))
                } else {
                    Ok(Transformed::no(Expr::ScalarFunction(scalar_function)))
                }
            }
            BuiltinName::Equal => {
                let left_is_column = matches!(&scalar_function.args[0], Expr::Column(_));
                let right_is_column = matches!(&scalar_function.args[1], Expr::Column(_));

                let left_is_resource = is_resource_in_encoding(&scalar_function.args[0]);
                let right_is_resource = is_resource_in_encoding(&scalar_function.args[1]);

                if left_is_column && right_is_resource.is_some()
                    || left_is_resource.is_some() && right_is_column
                    || left_is_resource.is_some() && right_is_resource.is_some()
                {
                    let [lhs, rhs] = TryInto::<[Expr; 2]>::try_into(scalar_function.args)
                        .map_err(|_| {
                            plan_datafusion_err!("Unexpected number of args for Equal")
                        })?;

                    let encoding_name =
                        left_is_resource.unwrap_or_else(|| right_is_resource.unwrap());
                    match encoding_name {
                        EncodingName::PlainTerm => {
                            let equality = lhs.eq(rhs);
                            let udf = self.registry.create_udf(
                                FunctionName::Builtin(
                                    BuiltinName::NativeBooleanAsTypedValue,
                                ),
                                RdfFusionFunctionArgs::empty(),
                            )?;
                            Ok(Transformed::yes(udf.call(vec![equality])))
                        }
                        EncodingName::TypedValue => {
                            let udf = self.registry.create_udf(
                                FunctionName::Builtin(BuiltinName::SameTerm),
                                RdfFusionFunctionArgs::empty(),
                            )?;
                            Ok(Transformed::yes(udf.call(vec![lhs, rhs])))
                        }
                        _ => unreachable!("These encodings are not supported."),
                    }
                } else {
                    Ok(Transformed::no(Expr::ScalarFunction(scalar_function)))
                }
            }
            _ => Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
        }
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
        let old_schema = plan.schema().clone();
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
        let plan = plan.map_expressions(|expr| {
            let name = name_preserver.save(&expr);
            let expr = self.try_rewrite_expression(expr, &schema)?;
            Ok(Transformed::new_transformed(
                name.restore(expr.data),
                expr.transformed,
            ))
        })?;

        check_same_schema(old_schema.as_ref(), plan.data.schema())?;
        Ok(plan)
    }
}

fn is_resource_in_encoding(expr: &Expr) -> Option<EncodingName> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };

    if let Ok(scalar) = PLAIN_TERM_ENCODING.try_new_scalar(value.clone()) {
        let term_type = scalar.term_type();
        return match term_type {
            Some(PlainTermType::NamedNode) | Some(PlainTermType::BlankNode) => {
                Some(EncodingName::PlainTerm)
            }
            _ => None,
        };
    }

    if let Ok(scalar) = TYPED_VALUE_ENCODING.try_new_scalar(value.clone()) {
        let term_type = scalar.term_type();
        return match term_type {
            Some(PlainTermType::NamedNode) | Some(PlainTermType::BlankNode) => {
                Some(EncodingName::TypedValue)
            }
            _ => None,
        };
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::{DFSchema, DFSchemaRef};
    use datafusion::logical_expr::{
        EmptyRelation, LogicalPlan, LogicalPlanBuilder, col, lit,
    };
    use datafusion::optimizer::OptimizerContext;
    use insta::assert_snapshot;
    use rdf_fusion_api::functions::{
        FunctionName, RdfFusionFunctionArgs, RdfFusionFunctionRegistry,
    };
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
    use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
    use rdf_fusion_encoding::{
        EncodingName, EncodingScalar, RdfFusionEncodings, TermEncoder,
    };
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
    use rdf_fusion_model::{NamedNodeRef, TypedValueRef};

    #[test]
    fn test_is_compatible_rewrite_when_not_nullable() {
        let schema = make_schema(EncodingName::PlainTerm, false, false);
        let rewritten = execute_test_for_builtin(&schema, BuiltinName::IsCompatible);
        assert_snapshot!(rewritten.data, @r"
        Projection: column1 = column2 AS IS_COMPATIBLE(column1,column2)
          EmptyRelation
        ");
    }

    #[test]
    fn test_is_compatible_does_not_rewrite_when_nullable() {
        let schema = make_schema(EncodingName::PlainTerm, false, true);
        let rewritten = execute_test_for_builtin(&schema, BuiltinName::IsCompatible);
        assert_snapshot!(rewritten.data, @r"
        Projection: IS_COMPATIBLE(column1, column2)
          EmptyRelation
        ");
    }

    #[test]
    fn test_equal_rewrite_when_one_side_is_resource_typed_value() {
        let schema = make_schema(EncodingName::TypedValue, false, true);
        let resource = DefaultTypedValueEncoder::encode_term(Ok(
            TypedValueRef::NamedNode(NamedNodeRef::new_unchecked("www.example.com/a")),
        ))
        .unwrap();
        let rewritten = execute_test_for_builtin_with_args(
            &schema,
            BuiltinName::Equal,
            vec![col("column1"), lit(resource.into_scalar_value())],
        );
        assert_snapshot!(rewritten.data, @r"
        Projection: SAMETERM(column1, Union 1:www.example.com/a) AS EQ(column1,Union 1:www.example.com/a)
          EmptyRelation
        ");
    }

    #[test]
    fn test_equal_does_not_rewrite_when_one_side_is_rdf_literal() {
        let schema = make_schema(EncodingName::TypedValue, false, true);
        let boolean_lit = DefaultTypedValueEncoder::encode_term(Ok(
            TypedValueRef::BooleanLiteral(true.into()),
        ))
        .unwrap();
        let rewritten = execute_test_for_builtin_with_args(
            &schema,
            BuiltinName::Equal,
            vec![col("column1"), lit(boolean_lit.into_scalar_value())],
        );
        assert_snapshot!(rewritten.data, @r"
        Projection: EQ(column1, Union 4:true)
          EmptyRelation
        ");
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
        let registry = function_registry();
        let expr = Expr::ScalarFunction(ScalarFunction {
            func: registry
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
        let registry = function_registry();
        let plan = create_plan(&schema)
            .project(vec![expr])
            .unwrap()
            .build()
            .unwrap();
        let rule = SimplifySparqlExpressionsRule::new(Arc::new(registry));
        let rewritten = rule.rewrite(plan, &OptimizerContext::new()).unwrap();
        rewritten
    }

    fn function_registry() -> DefaultRdfFusionFunctionRegistry {
        let encodings = RdfFusionEncodings::new(
            PLAIN_TERM_ENCODING,
            TYPED_VALUE_ENCODING,
            None,
            SORTABLE_TERM_ENCODING,
        );
        DefaultRdfFusionFunctionRegistry::new(encodings)
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
