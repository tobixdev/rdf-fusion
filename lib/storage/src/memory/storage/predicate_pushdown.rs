use crate::memory::object_id::EncodedObjectId;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
};
use std::any::Any;
use std::sync::Arc;

/// Represents the set of supported operations for [MemStoragePredicateExpr]s.
pub enum PredicateExprOperator {
    Gt,
    Geq,
    Lt,
    Leq,
    Eq,
}

/// Represents a predicate that has been pushed down and is supported by our implementation.
pub enum MemStoragePredicateExpr {
    /// The filter is always true.
    True,
    /// A reference to a column.
    Column(Arc<str>),
    /// An object id.
    ObjectId(EncodedObjectId),
    /// Applies a [PredicateExprOperator] to a column and a scalar.
    Binary(Arc<str>, PredicateExprOperator, EncodedObjectId),
    /// Checks that a column is between two object ids.
    Between(Arc<str>, EncodedObjectId, EncodedObjectId),
    /// A reference to a dynamic filter that can be updated during execution.
    DynamicFilter(Arc<DynamicFilterPhysicalExpr>),
}

impl MemStoragePredicateExpr {
    /// Tries to create a [MemStoragePredicateExpr] from an arbitrary [PhysicalExpr].
    ///
    /// If [None] is returned, the expression was not supported.
    pub fn try_from(schema: &Schema, expr: &Arc<dyn PhysicalExpr>) -> Option<Self> {
        let any_expr = Arc::clone(expr) as Arc<dyn Any + Send + Sync>;
        if let Ok(expr) = any_expr.downcast::<DynamicFilterPhysicalExpr>() {
            return Some(MemStoragePredicateExpr::DynamicFilter(expr));
        }

        try_rewrite_data_fusion_expr(schema, expr)
    }
}

/// Tries to rewrite an arbitrary DataFusion [PhysicalExpr] into a supported
/// [MemStoragePredicateExpr].
pub fn try_rewrite_data_fusion_expr(
    schema: &Schema,
    expr: &Arc<dyn PhysicalExpr>,
) -> Option<MemStoragePredicateExpr> {
    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        return Some(MemStoragePredicateExpr::Column(
            column.name().to_owned().into(),
        ));
    }

    if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
        return match lit.value() {
            ScalarValue::UInt32(Some(value)) => {
                Some(MemStoragePredicateExpr::ObjectId((*value).into()))
            }
            ScalarValue::Boolean(Some(true)) => Some(MemStoragePredicateExpr::True),
            _ => None,
        };
    }

    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        return match binary.op() {
            Operator::Eq => {
                let left = try_rewrite_data_fusion_expr(schema, binary.left())?;
                let right = try_rewrite_data_fusion_expr(schema, binary.right())?;

                match (left, right) {
                    (
                        MemStoragePredicateExpr::Column(left),
                        MemStoragePredicateExpr::ObjectId(right),
                    )
                    | (
                        MemStoragePredicateExpr::ObjectId(right),
                        MemStoragePredicateExpr::Column(left),
                    ) => Some(MemStoragePredicateExpr::Binary(
                        left,
                        PredicateExprOperator::Eq,
                        right,
                    )),
                    _ => return None,
                }
            }
            _ => return None,
        };
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field};

    #[test]
    fn test_column_predicate() {
        let schema = test_schema();
        let expr = column_expr("subject");
        let result = try_rewrite_data_fusion_expr(&schema, &expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::Column(_))));
    }

    #[test]
    fn test_literal_object_id() {
        let schema = test_schema();
        let expr = literal_uint(42);
        let result = try_rewrite_data_fusion_expr(&schema, &expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::ObjectId(_))));
    }

    #[test]
    fn test_true_literal() {
        let schema = test_schema();
        let expr = literal_bool(true);
        let result = try_rewrite_data_fusion_expr(&schema, &expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::True)));
    }

    #[test]
    fn test_equal_predicate() {
        let schema = test_schema();
        let left = column_expr("subject");
        let right = literal_uint(123);
        let expr =
            Arc::new(BinaryExpr::new(left, Operator::Eq, right)) as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_data_fusion_expr(&schema, &expr);

        assert!(matches!(
            result,
            Some(MemStoragePredicateExpr::Binary(
                _,
                PredicateExprOperator::Eq,
                _
            ))
        ));
    }

    #[test]
    fn test_between_predicate() {
        let schema = test_schema();
        let gt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::Gt,
            literal_uint(123),
        )) as Arc<dyn PhysicalExpr>;
        let lt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::Lt,
            literal_uint(456),
        )) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(gt_expr, Operator::And, lt_expr))
            as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_data_fusion_expr(&schema, &expr).unwrap();
        let MemStoragePredicateExpr::Between(column, from, to) = result else {
            panic!("Unexpected expr.")
        };

        assert_eq!(column.as_ref(), "subject");
        assert_eq!(from.as_u32(), 123);
        assert_eq!(to.as_u32(), 456);
    }

    #[test]
    fn test_between_predicate_wrong_column_name() {
        let schema = test_schema();
        let gt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::Gt,
            literal_uint(123),
        )) as Arc<dyn PhysicalExpr>;
        let lt_expr = Arc::new(BinaryExpr::new(
            column_expr("predicate"),
            Operator::Lt,
            literal_uint(456),
        )) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(gt_expr, Operator::And, lt_expr))
            as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_data_fusion_expr(&schema, &expr);

        assert!(matches!(result, None));
    }

    #[test]
    fn test_unsupported_operator() {
        let schema = test_schema();
        let left = column_expr("subject");
        let right = literal_uint(123);
        let expr =
            Arc::new(BinaryExpr::new(left, Operator::Gt, right)) as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_data_fusion_expr(&schema, &expr);

        assert!(result.is_none());
    }

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("subject", DataType::UInt32, false),
            Field::new("predicate", DataType::UInt32, false),
            Field::new("object", DataType::UInt32, false),
        ])
    }

    fn column_expr(name: &str) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new(name, 0))
    }

    fn literal_uint(value: u32) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::UInt32(Some(value))))
    }

    fn literal_bool(value: bool) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::Boolean(Some(value))))
    }
}
