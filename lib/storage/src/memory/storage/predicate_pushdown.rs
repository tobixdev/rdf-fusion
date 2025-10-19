use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::{
    IndexScanPredicate, IndexScanPredicateSource, PossiblyDynamicIndexScanPredicate,
};
use datafusion::common::{ScalarValue, exec_datafusion_err, exec_err};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
};
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::collections::BTreeSet;
use std::sync::Arc;
use thiserror::Error;

/// Represents the set of supported operations for [MemStoragePredicateExpr]s.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PredicateExprOperator {
    Gt,
    GtEq,
    Lt,
    LtEq,
    Eq,
}

impl PredicateExprOperator {
    /// Flips the operator. For example, `>` becomes ´<`.
    pub fn flip(self) -> Self {
        match self {
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Eq => Self::Eq,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
#[error("Unsupported operator.")]
pub struct UnsupportedOperatorError;

impl TryFrom<Operator> for PredicateExprOperator {
    type Error = UnsupportedOperatorError;

    fn try_from(value: Operator) -> Result<Self, Self::Error> {
        Ok(match value {
            Operator::Lt => PredicateExprOperator::Lt,
            Operator::LtEq => PredicateExprOperator::LtEq,
            Operator::Gt => PredicateExprOperator::Gt,
            Operator::GtEq => PredicateExprOperator::GtEq,
            Operator::Eq => PredicateExprOperator::Eq,
            _ => return Err(UnsupportedOperatorError),
        })
    }
}

/// Represents a predicate that has been pushed down and is supported by our implementation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// Holds a dynamic filter that will be evaluated during query execution.
    Dynamic(Arc<DynamicFilterPhysicalExpr>),
}

impl MemStoragePredicateExpr {
    /// Tries to create a [MemStoragePredicateExpr] from an arbitrary [PhysicalExpr].
    ///
    /// If [None] is returned, the expression was not supported.
    pub fn try_from(expr: &Arc<dyn PhysicalExpr>) -> Option<Self> {
        let any_expr = Arc::clone(expr) as Arc<dyn Any + Send + Sync>;
        if let Ok(expr) = any_expr.downcast::<DynamicFilterPhysicalExpr>() {
            return Some(MemStoragePredicateExpr::Dynamic(expr));
        }

        try_rewrite_datafusion_expr(expr)
    }

    /// Returns the name of the column that this predicate operates on.
    ///
    /// Will be [None] for expressions that do not have a column name.
    pub fn column(&self) -> Option<&str> {
        match self {
            Self::Column(column) => Some(column.as_ref()),
            Self::Binary(column, _, _) => Some(column.as_ref()),
            Self::Between(column, _, _) => Some(column.as_ref()),
            _ => None,
        }
    }

    /// Returns the [IndexScanPredicate] that implements this expression.
    ///
    /// Returns [None] if the expression always evaluates to true.
    /// Returns [Err] if the expression is not a predicate.
    pub fn to_scan_predicate(
        &self,
    ) -> DFResult<Option<PossiblyDynamicIndexScanPredicate>> {
        use IndexScanPredicate::*;
        use PossiblyDynamicIndexScanPredicate::*;

        Ok(match self {
            MemStoragePredicateExpr::True => None,

            MemStoragePredicateExpr::Binary(_, operator, value) => Some(match operator {
                PredicateExprOperator::Gt => {
                    let Some(next) = value.next() else {
                        return Ok(Some(Static(False)));
                    };
                    Static(Between(next, EncodedObjectId::MAX))
                }
                PredicateExprOperator::GtEq => {
                    Static(Between(*value, EncodedObjectId::MAX))
                }
                PredicateExprOperator::Lt => {
                    let Some(previous) = value.previous() else {
                        return Ok(Some(Static(False)));
                    };
                    Static(Between(EncodedObjectId::MIN, previous))
                }
                PredicateExprOperator::LtEq => {
                    Static(Between(EncodedObjectId::MIN, *value))
                }
                PredicateExprOperator::Eq => Static(In(BTreeSet::from([*value]))),
            }),
            MemStoragePredicateExpr::Between(_, from, to) => {
                Some(Static(Between(*from, *to)))
            }
            MemStoragePredicateExpr::Dynamic(expr) => Some(Dynamic(
                Arc::clone(expr) as Arc<dyn IndexScanPredicateSource>
            )),

            MemStoragePredicateExpr::Column(_) | MemStoragePredicateExpr::ObjectId(_) => {
                return exec_err!("Expression is not a predicate.");
            }
        })
    }
}

/// Tries to rewrite an arbitrary DataFusion [PhysicalExpr] into a supported
/// [MemStoragePredicateExpr].
pub fn try_rewrite_datafusion_expr(
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
        let left = try_rewrite_datafusion_expr(binary.left())?;
        let right = try_rewrite_datafusion_expr(binary.right())?;

        return match binary.op() {
            Operator::Eq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq => {
                let op = PredicateExprOperator::try_from(*binary.op()).ok()?;

                let (column, literal, op) = match (left, right) {
                    (
                        MemStoragePredicateExpr::Column(left),
                        MemStoragePredicateExpr::ObjectId(right),
                    ) => (left, right, op),
                    (
                        MemStoragePredicateExpr::ObjectId(right),
                        MemStoragePredicateExpr::Column(left),
                    ) => (left, right, op.flip()),
                    _ => return None,
                };

                Some(MemStoragePredicateExpr::Binary(column, op, literal))
            }
            Operator::And => {
                let MemStoragePredicateExpr::Binary(lhs_column, _, _) = &left else {
                    return None;
                };
                let MemStoragePredicateExpr::Binary(rhs_column, _, _) = &right else {
                    return None;
                };

                if lhs_column != rhs_column {
                    return None;
                }

                let left = left.to_scan_predicate().ok().flatten()?;
                let right = right.to_scan_predicate().ok().flatten()?;
                return match left.try_and_with(&right)? {
                    PossiblyDynamicIndexScanPredicate::Static(
                        IndexScanPredicate::Between(from, to),
                    ) => Some(MemStoragePredicateExpr::Between(
                        Arc::clone(lhs_column),
                        from,
                        to,
                    )),
                    _ => None,
                };
            }
            _ => return None,
        };
    }

    None
}

impl IndexScanPredicateSource for DynamicFilterPhysicalExpr {
    fn current_predicate(&self) -> DFResult<Option<IndexScanPredicate>> {
        let expr = self.current()?;
        let expr = try_rewrite_datafusion_expr(&expr)
            .ok_or_else(|| exec_datafusion_err!("Unsupported predicate."))?;
        match expr.to_scan_predicate()? {
            None => Ok(None),
            Some(PossiblyDynamicIndexScanPredicate::Static(predicate)) => {
                Ok(Some(predicate))
            }
            _ => exec_err!("Unsupported predicate."),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use IndexScanPredicate::*;
    use PossiblyDynamicIndexScanPredicate::*;

    #[test]
    fn test_column_predicate() {
        let expr = column_expr("subject");
        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::Column(_))));
    }

    #[test]
    fn test_literal_object_id() {
        let expr = literal_uint(42);
        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::ObjectId(_))));
    }

    #[test]
    fn test_true_literal() {
        let expr = literal_bool(true);
        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, Some(MemStoragePredicateExpr::True)));
    }

    #[test]
    fn test_equal_predicate() {
        let left = column_expr("subject");
        let right = literal_uint(123);
        let expr =
            Arc::new(BinaryExpr::new(left, Operator::Eq, right)) as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_datafusion_expr(&expr);

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
        let gt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::Gt,
            literal_uint(123),
        )) as Arc<dyn PhysicalExpr>;
        let lt_expr = Arc::new(BinaryExpr::new(
            column_expr("subject"),
            Operator::LtEq,
            literal_uint(456),
        )) as Arc<dyn PhysicalExpr>;
        let expr = Arc::new(BinaryExpr::new(gt_expr, Operator::And, lt_expr))
            as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_datafusion_expr(&expr).unwrap();
        let MemStoragePredicateExpr::Between(column, from, to) = result else {
            panic!("Unexpected expr.")
        };

        assert_eq!(column.as_ref(), "subject");
        assert_eq!(from.as_u32(), 124);
        assert_eq!(to.as_u32(), 456);
    }

    #[test]
    fn test_between_predicate_wrong_column_name() {
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

        let result = try_rewrite_datafusion_expr(&expr);

        assert!(matches!(result, None));
    }

    #[test]
    fn test_unsupported_operator() {
        let left = column_expr("subject");
        let right = literal_uint(123);
        let expr = Arc::new(BinaryExpr::new(left, Operator::Plus, right))
            as Arc<dyn PhysicalExpr>;

        let result = try_rewrite_datafusion_expr(&expr);

        assert!(result.is_none());
    }

    #[test]
    fn test_to_scan_predicate_true_returns_none() {
        let result = MemStoragePredicateExpr::True.to_scan_predicate().unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_to_scan_predicate_eq_produces_in_predicate() {
        let obj_id = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Eq,
            obj_id,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Static(In(BTreeSet::from([obj_id]))))
        );
    }

    #[test]
    fn test_to_scan_predicate_gt_produces_between() {
        let obj_id = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Gt,
            obj_id,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Static(Between(
                EncodedObjectId::from(101),
                EncodedObjectId::MAX
            )))
        );
    }

    #[test]
    fn test_to_scan_predicate_geq_produces_between() {
        let obj_id = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::GtEq,
            obj_id,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Static(Between(
                EncodedObjectId::from(100),
                EncodedObjectId::MAX
            )))
        );
    }

    #[test]
    fn test_to_scan_predicate_lt_produces_between() {
        let value = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Lt,
            value,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Static(Between(
                EncodedObjectId::MIN,
                EncodedObjectId::from(99)
            )))
        );
    }

    #[test]
    fn test_to_scan_predicate_leq_produces_between() {
        use crate::memory::object_id::EncodedObjectId;
        let value = EncodedObjectId::from(100);
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::LtEq,
            value,
        );

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Static(Between(
                EncodedObjectId::MIN,
                EncodedObjectId::from(100)
            )))
        );
    }

    #[test]
    fn test_to_scan_predicate_between_expr_produces_between() {
        use crate::memory::object_id::EncodedObjectId;
        let from = EncodedObjectId::from(10);
        let to = EncodedObjectId::from(20);
        let expr = MemStoragePredicateExpr::Between(Arc::from("col"), from, to);

        assert_eq!(
            expr.to_scan_predicate().unwrap(),
            Some(Static(Between(from, to)))
        );
    }

    #[test]
    fn test_to_scan_predicate_gt_with_max_value_returns_false() {
        use crate::memory::object_id::EncodedObjectId;
        let max_id = EncodedObjectId::MAX;
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Gt,
            max_id,
        );

        assert_eq!(expr.to_scan_predicate().unwrap(), Some(Static(False)));
    }

    #[test]
    fn test_to_scan_predicate_lt_with_min_value_returns_false() {
        use crate::memory::object_id::EncodedObjectId;
        let min_id = EncodedObjectId::MIN;
        let expr = MemStoragePredicateExpr::Binary(
            Arc::from("col"),
            PredicateExprOperator::Lt,
            min_id,
        );

        assert_eq!(expr.to_scan_predicate().unwrap(), Some(Static(False)));
    }

    #[test]
    fn test_to_scan_predicate_column_is_err() {
        let expr = MemStoragePredicateExpr::Column(Arc::from("col"));
        let result = expr.to_scan_predicate();
        assert!(result.is_err());
    }

    #[test]
    fn test_to_scan_predicate_objectid_is_err() {
        use crate::memory::object_id::EncodedObjectId;
        let expr = MemStoragePredicateExpr::ObjectId(EncodedObjectId::from(1));
        let result = expr.to_scan_predicate();
        assert!(result.is_err());
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
