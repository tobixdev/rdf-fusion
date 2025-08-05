use crate::{check_same_schema, RdfFusionExprBuilderContext};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{plan_datafusion_err, DFSchema, DFSchemaRef};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::utils::merge_schema;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::utils::NamePreserver;
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_api::functions::{BuiltinName, FunctionName, RdfFusionFunctionArgs};
use rdf_fusion_api::RdfFusionContextView;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::{PlainTermType, PLAIN_TERM_ENCODING};
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use std::sync::Arc;

/// An optimizer rule that tries to optimize SPARQL expressions.
#[derive(Debug)]
pub struct SimplifySparqlExpressionsRule {
    /// Used for constructing new expressions.
    context: RdfFusionContextView,
}

impl SimplifySparqlExpressionsRule {
    /// Creates a new [SimplifySparqlExpressionsRule].
    pub fn new(context: RdfFusionContextView) -> Self {
        Self { context }
    }

    /// Rewrites an [ExtendNode] into a projection.
    fn try_rewrite_expression(
        &self,
        expr: Expr,
        input_schema: &DFSchema,
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
        input_schema: &DFSchema,
    ) -> DFResult<Transformed<Expr>> {
        let function_name = scalar_function.func.name();
        let Ok(built_in) = BuiltinName::try_from(function_name) else {
            return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
        };

        match built_in {
            BuiltinName::IsCompatible => {
                Self::try_rewrite_is_compatible(scalar_function, input_schema)
            }
            BuiltinName::Equal => self.try_rewrite_equal(scalar_function, input_schema),
            _ => Ok(Transformed::no(Expr::ScalarFunction(scalar_function))),
        }
    }

    fn try_rewrite_is_compatible(
        scalar_function: ScalarFunction,
        input_schema: &DFSchema,
    ) -> DFResult<Transformed<Expr>> {
        let lhs_nullable = scalar_function.args[0].nullable(input_schema)?;
        let rhs_nullable = scalar_function.args[1].nullable(input_schema)?;

        if !lhs_nullable && !rhs_nullable {
            let [lhs, rhs] = TryInto::<[Expr; 2]>::try_into(scalar_function.args)
                .map_err(|_| {
                    plan_datafusion_err!("Unexpected number of args for IS_COMPATIBLE")
                })?;
            Ok(Transformed::yes(lhs.eq(rhs)))
        } else {
            Ok(Transformed::no(Expr::ScalarFunction(scalar_function)))
        }
    }

    fn try_rewrite_equal(
        &self,
        scalar_function: ScalarFunction,
        input_schema: &DFSchema,
    ) -> DFResult<Transformed<Expr>> {
        let left_is_column = is_column_or_encoded_column(&scalar_function.args[0]);
        let right_is_column = is_column_or_encoded_column(&scalar_function.args[1]);

        let left_resource = is_resource_or_encoded_resource(&scalar_function.args[0]);
        let right_resource = is_resource_or_encoded_resource(&scalar_function.args[1]);

        let can_be_rewritten = left_is_column && right_resource
            || left_resource && right_is_column
            || left_resource && right_resource;
        if !can_be_rewritten {
            return Ok(Transformed::no(Expr::ScalarFunction(scalar_function)));
        }

        let [lhs, rhs] = TryInto::<[Expr; 2]>::try_into(scalar_function.args)
            .map_err(|_| plan_datafusion_err!("Unexpected number of args for Equal"))?;
        let lhs = try_unwrap_encoding(lhs)?;
        let rhs = try_unwrap_encoding(rhs)?;

        let (lhs_data_type, _) = lhs.data_type_and_nullable(input_schema)?;
        let (rhs_data_type, _) = rhs.data_type_and_nullable(input_schema)?;

        let lhs_encoding = self
            .context
            .encodings()
            .try_get_encoding_name(&lhs_data_type)
            .expect("Only works for RDF terms");
        let rhs_encoding = self
            .context
            .encodings()
            .try_get_encoding_name(&rhs_data_type)
            .expect("Only works for RDF terms");

        let target_encoding =
            match (left_resource, lhs_encoding, right_resource, rhs_encoding) {
                // If they share
                (_, encoding_a, _, encoding_b) if encoding_a == encoding_b => encoding_a,
                // If only one of the two columns is a literal, prefer to change the other one.
                (true, lit_encoding, false, _) => lit_encoding,
                (false, _, true, lit_encoding) => lit_encoding,
                // Other choose Plain Term to hope for a "=" operator that hopefully triggers something
                _ => EncodingName::PlainTerm,
            };

        let expr_builder = RdfFusionExprBuilderContext::new(&self.context, input_schema);
        let lhs = expr_builder
            .try_create_builder(lhs)?
            .with_encoding(target_encoding)?
            .build()?;
        let rhs = expr_builder
            .try_create_builder(rhs)?
            .with_encoding(target_encoding)?
            .build()?;

        match target_encoding {
            EncodingName::PlainTerm => {
                let equality = lhs.eq(rhs);
                let udf = self.context.functions().create_udf(
                    FunctionName::Builtin(BuiltinName::NativeBooleanAsTypedValue),
                    RdfFusionFunctionArgs::empty(),
                )?;
                Ok(Transformed::yes(udf.call(vec![equality])))
            }
            EncodingName::TypedValue => {
                let expr = expr_builder
                    .try_create_builder(lhs)?
                    .same_term(rhs)?
                    .with_encoding(EncodingName::TypedValue)?
                    .build()?;
                Ok(Transformed::yes(expr))
            }
            _ => unreachable!("These encodings are not supported."),
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

fn is_column_or_encoded_column(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::ScalarFunction(function) => {
            let built_in = BuiltinName::try_from(function.func.name());
            matches!(built_in, Ok(BuiltinName::WithTypedValueEncoding))
        }
        _ => false,
    }
}

fn is_resource_or_encoded_resource(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => {
            if let Ok(scalar) = PLAIN_TERM_ENCODING.try_new_scalar(value.clone()) {
                let term_type = scalar.term_type();
                return match term_type {
                    Some(PlainTermType::NamedNode) | Some(PlainTermType::BlankNode) => {
                        true
                    }
                    _ => false,
                };
            }

            if let Ok(scalar) = TYPED_VALUE_ENCODING.try_new_scalar(value.clone()) {
                let term_type = scalar.term_type();
                return match term_type {
                    Some(PlainTermType::NamedNode) | Some(PlainTermType::BlankNode) => {
                        true
                    }
                    _ => false,
                };
            }

            false
        }
        Expr::ScalarFunction(function) => {
            let built_in = BuiltinName::try_from(function.func.name());
            match built_in {
                Ok(BuiltinName::WithTypedValueEncoding) => {
                    is_resource_or_encoded_resource(&function.args[0])
                }
                _ => false,
            }
        }
        _ => false,
    }
}

fn try_unwrap_encoding(expr: Expr) -> DFResult<Expr> {
    match expr {
        Expr::ScalarFunction(function) => {
            let built_in_lhs = BuiltinName::try_from(function.func.name());
            match built_in_lhs {
                Ok(BuiltinName::WithTypedValueEncoding) => {
                    let [arg] =
                        TryInto::<[Expr; 1]>::try_into(function.args).map_err(|_| {
                            plan_datafusion_err!(
                                "Unexpected number of args for WithTypedValueEncoding"
                            )
                        })?;
                    try_unwrap_encoding(arg)
                }
                _ => Ok(Expr::ScalarFunction(function)),
            }
        }
        expr => Ok(expr),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::{DFSchema, DFSchemaRef};
    use datafusion::logical_expr::{
        col, lit, EmptyRelation, LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion::optimizer::OptimizerContext;
    use insta::assert_snapshot;
    use rdf_fusion_api::functions::{FunctionName, RdfFusionFunctionArgs};
    use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
    use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
    use rdf_fusion_encoding::{
        EncodingName, EncodingScalar, QuadStorageEncoding, RdfFusionEncodings,
        TermEncoder,
    };
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
    use rdf_fusion_model::{NamedNodeRef, TermRef, TypedValueRef};

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
    fn test_equal_rewrite_when_one_side_is_resource_plain_term() {
        let schema = make_schema(EncodingName::PlainTerm, false, true);
        let resource = DefaultPlainTermEncoder::encode_term(Ok(TermRef::NamedNode(
            NamedNodeRef::new_unchecked("www.example.com/a"),
        )))
        .unwrap();

        let context = create_context();
        let with_typed_value_encoding = context
            .functions()
            .create_udf(
                FunctionName::Builtin(BuiltinName::WithTypedValueEncoding),
                RdfFusionFunctionArgs::empty(),
            )
            .unwrap();

        let rewritten = execute_test_for_builtin_with_args(
            &schema,
            BuiltinName::Equal,
            vec![
                with_typed_value_encoding.call(vec![col("column1")]),
                with_typed_value_encoding.call(vec![lit(resource.into_scalar_value())]),
            ],
        );
        assert_snapshot!(rewritten.data, @ r"
        Projection: BOOLEAN_AS_TERM(column1 = Struct({term_type:0,value:www.example.com/a,data_type:,language_tag:})) AS EQ(WITH_TYPED_VALUE_ENCODING(column1),WITH_TYPED_VALUE_ENCODING(Struct({term_type:0,value:www.example.com/a,data_type:,language_tag:})))
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
        let rule = SimplifySparqlExpressionsRule::new(create_context());
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
