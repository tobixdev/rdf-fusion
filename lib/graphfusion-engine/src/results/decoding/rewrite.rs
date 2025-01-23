use crate::results::decoding::logical::DecodeRdfTermsNode;
use crate::DFResult;
use arrow_rdf::encoded::{ENC_DECODE, ENC_TYPE_TERM};
use datafusion::arrow::datatypes::Field;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, Projection, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct DecodeRdfTermsToProjectionRule {}

impl AnalyzerRule for DecodeRdfTermsToProjectionRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        Self::analyze_plan(plan)
    }

    fn name(&self) -> &str {
        "decode_rdf_terms_to_projection_rule"
    }
}

impl DecodeRdfTermsToProjectionRule {
    fn analyze_plan(plan: LogicalPlan) -> DFResult<LogicalPlan> {
        Ok(plan
            .transform(|plan| {
                let new_plan = match plan {
                    LogicalPlan::Extension(Extension { node })
                        if node.as_any().downcast_ref::<DecodeRdfTermsNode>().is_some() =>
                    {
                        let node = node.as_any().downcast_ref::<DecodeRdfTermsNode>().unwrap();
                        let input = node.inputs()[0].clone();
                        let expressions = input
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| field_to_expr(f))
                            .collect();
                        Transformed::yes(LogicalPlan::Projection(Projection::try_new(
                            expressions,
                            Arc::new(input),
                        )?))
                    }
                    _ => Transformed::no(plan),
                };
                Ok(new_plan)
            })?
            .data)
    }
}

fn field_to_expr(field: &Field) -> Expr {
    if *field.data_type() == *ENC_TYPE_TERM {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(ENC_DECODE.clone()),
            vec![Expr::Column(Column::from(field.name().clone()))],
        ))
    } else {
        Expr::Column(Column::from_name(field.name()))
    }
}
