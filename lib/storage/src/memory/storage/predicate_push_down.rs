use crate::memory::object_id::EncodedObjectId;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, DynamicFilterPhysicalExpr, Literal,
};
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::sync::Arc;

pub enum RewrittenPushedDownPredicate {
    True,
    Column(Arc<str>),
    ObjectId(EncodedObjectId),
    Equal(Arc<str>, EncodedObjectId),
    Between(Arc<str>, EncodedObjectId, EncodedObjectId),
    DynamicFilter(Arc<DynamicFilterPhysicalExpr>),
}

/// TODO
pub fn supports_push_down(
    schema: &Schema,
    expr: &Arc<dyn PhysicalExpr>,
) -> DFResult<Option<RewrittenPushedDownPredicate>> {
    let any_expr = expr.clone() as Arc<dyn Any + Send + Sync>;
    if let Ok(expr) = any_expr.downcast::<DynamicFilterPhysicalExpr>() {
        return Ok(Some(RewrittenPushedDownPredicate::DynamicFilter(expr)));
    }

    Ok(try_rewrite_data_fusion_expr(schema, expr))
}

/// TODO
pub fn try_rewrite_data_fusion_expr(
    schema: &Schema,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<RewrittenPushedDownPredicate> {
    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        return Some(RewrittenPushedDownPredicate::Column(
            column.name().to_owned().into(),
        ));
    }

    if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
        return match lit.value() {
            ScalarValue::UInt32(Some(value)) => {
                Some(RewrittenPushedDownPredicate::ObjectId((*value).into()))
            }
            ScalarValue::Boolean(Some(true)) => Some(RewrittenPushedDownPredicate::True),
            _ => None,
        };
    }

    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        return match binary.op() {
            Operator::Eq => {
                let left = try_rewrite_data_fusion_expr(schema, &binary.left())?;
                let right = try_rewrite_data_fusion_expr(schema, &binary.right())?;

                match (left, right) {
                    (
                        RewrittenPushedDownPredicate::Column(left),
                        RewrittenPushedDownPredicate::ObjectId(right),
                    )
                    | (
                        RewrittenPushedDownPredicate::ObjectId(right),
                        RewrittenPushedDownPredicate::Column(left),
                    ) => Some(RewrittenPushedDownPredicate::Equal(left, right)),
                    _ => return None,
                }
            }
            _ => return None,
        };
    }

    None
}
