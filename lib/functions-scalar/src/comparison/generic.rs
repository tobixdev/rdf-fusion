use crate::ScalarBinaryRdfOp;
use crate::ThinResult;
use model::Boolean;
use model::InternalTermRef;

macro_rules! create_binary_cmp_udf {
    ($STRUCT: ident, $OP: tt) => {
        #[derive(Debug)]
        pub struct $STRUCT {
        }

        impl Default for $STRUCT {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $STRUCT {
            pub fn new() -> Self {
                Self {}
            }
        }

        impl ScalarBinaryRdfOp for $STRUCT {
            type ArgLhs<'data> = InternalTermRef<'data>;
            type ArgRhs<'data> = InternalTermRef<'data>;
            type Result<'data> = Boolean;

            fn evaluate<'data>(
                &self,
                lhs: Self::ArgLhs<'data>,
                rhs: Self::ArgRhs<'data>,
            ) -> ThinResult<Self::Result<'data>> {
                let result = match (lhs, rhs) {
                    (InternalTermRef::NamedNode(l), InternalTermRef::NamedNode(r)) => l $OP r,
                    (InternalTermRef::BooleanLiteral(l), InternalTermRef::BooleanLiteral(r)) => l $OP r,
                    (InternalTermRef::NumericLiteral(l), InternalTermRef::NumericLiteral(r)) => l $OP r,
                    (InternalTermRef::SimpleLiteral(l), InternalTermRef::SimpleLiteral(r)) => l $OP r,
                    (InternalTermRef::LanguageStringLiteral(l), InternalTermRef::LanguageStringLiteral(r)) => l $OP r,
                    (InternalTermRef::DateTimeLiteral(l), InternalTermRef::DateTimeLiteral(r)) => l $OP r,
                    (InternalTermRef::DateLiteral(l), InternalTermRef::DateLiteral(r)) => l $OP r,
                    (InternalTermRef::TimeLiteral(l), InternalTermRef::TimeLiteral(r)) => l $OP r,
                    (InternalTermRef::DurationLiteral(l), InternalTermRef::DurationLiteral(r)) => l $OP r,
                    (InternalTermRef::YearMonthDurationLiteral(l), InternalTermRef::YearMonthDurationLiteral(r)) => l $OP r,
                    (InternalTermRef::DayTimeDurationLiteral(l), InternalTermRef::DayTimeDurationLiteral(r)) => l $OP r,
                    (InternalTermRef::TypedLiteral(l), InternalTermRef::TypedLiteral(r)) => l $OP r,
                    _ => false,
                };
                Ok(result.into())
            }
        }
    }
}

create_binary_cmp_udf!(GreaterThanRdfOp, >);
create_binary_cmp_udf!(GreaterOrEqualRdfOp, >=);
create_binary_cmp_udf!(LessThanRdfOp, <);
create_binary_cmp_udf!(LessOrEqualRdfOp, <=);

#[cfg(test)]
mod tests {
    use crate::comparison::generic::LessThanRdfOp;
    use crate::ScalarBinaryRdfOp;
    use model::{Numeric, InternalTermRef};

    #[test]
    fn test_lth_int_with_float() {
        let less_than = LessThanRdfOp::new();
        let result = less_than
            .evaluate(
                InternalTermRef::NumericLiteral(Numeric::Int(5.into())),
                InternalTermRef::NumericLiteral(Numeric::Float(10.0.into())),
            )
            .unwrap();
        assert_eq!(result, false.into());
    }
}
