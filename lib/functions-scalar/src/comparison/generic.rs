use crate::RdfOpResult;
use crate::ScalarBinaryRdfOp;
use datamodel::Boolean;
use datamodel::TermRef;

macro_rules! create_binary_cmp_udf {
    ($STRUCT: ident, $OP: tt) => {
        #[derive(Debug)]
        pub struct $STRUCT {
        }

        impl $STRUCT {
            pub fn new() -> Self {
                Self {}
            }
        }

        impl ScalarBinaryRdfOp for $STRUCT {
            type ArgLhs<'data> = TermRef<'data>;
            type ArgRhs<'data> = TermRef<'data>;
            type Result<'data> = Boolean;

            fn evaluate<'data>(
                &self,
                lhs: Self::ArgLhs<'data>,
                rhs: Self::ArgRhs<'data>,
            ) -> RdfOpResult<Self::Result<'data>> {
                let result = match (lhs, rhs) {
                    (TermRef::NamedNode(l), TermRef::NamedNode(r)) => l $OP r,
                    (TermRef::BooleanLiteral(l), TermRef::BooleanLiteral(r)) => l $OP r,
                    (TermRef::NumericLiteral(l), TermRef::NumericLiteral(r)) => l $OP r,
                    (TermRef::SimpleLiteral(l), TermRef::SimpleLiteral(r)) => l $OP r,
                    (TermRef::LanguageStringLiteral(l), TermRef::LanguageStringLiteral(r)) => l $OP r,
                    (TermRef::DurationLiteral(l), TermRef::DurationLiteral(r)) => l $OP r,
                    (TermRef::YearMonthDurationLiteral(l), TermRef::YearMonthDurationLiteral(r)) => l $OP r,
                    (TermRef::DayTimeDurationLiteral(l), TermRef::DayTimeDurationLiteral(r)) => l $OP r,
                    (TermRef::TypedLiteral(l), TermRef::TypedLiteral(r)) => l $OP r,
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
    use datamodel::{Numeric, TermRef};

    #[test]
    fn test_lth_int_with_float() {
        let less_than = LessThanRdfOp::new();
        let result = less_than
            .evaluate(
                TermRef::NumericLiteral(Numeric::Int(5.into())),
                TermRef::NumericLiteral(Numeric::Float(10.into())),
            )
            .unwrap();
        assert_eq!(result, false.into());
    }
}
