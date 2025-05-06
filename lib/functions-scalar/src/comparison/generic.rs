use crate::ThinResult;
use crate::{BinaryTermValueOp, SparqlOp};
use graphfusion_model::ThinError;
use graphfusion_model::{Boolean, TermValueRef};
use std::cmp::Ordering;

macro_rules! create_binary_cmp_udf {
    ($STRUCT: ident, $NAME: expr, $ORDERINGS: expr) => {
        #[derive(Debug)]
        pub struct $STRUCT {}

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

        impl SparqlOp for $STRUCT {
            fn name(&self) -> &str {
                $NAME
            }
        }

        impl BinaryTermValueOp for $STRUCT {
            type ArgLhs<'data> = TermValueRef<'data>;
            type ArgRhs<'data> = TermValueRef<'data>;
            type Result<'data> = Boolean;

            fn evaluate<'data>(
                &self,
                lhs: Self::ArgLhs<'data>,
                rhs: Self::ArgRhs<'data>,
            ) -> ThinResult<Self::Result<'data>> {
                lhs.partial_cmp(&rhs)
                    .map(|o| $ORDERINGS.contains(&o))
                    .map(Into::into)
                    .ok_or(ThinError::Expected)
            }
        }
    };
}

create_binary_cmp_udf!(EqSparqlOp, "eq", [Ordering::Equal]);
create_binary_cmp_udf!(GreaterThanSparqlOp, "gt", [Ordering::Greater]);
create_binary_cmp_udf!(
    GreaterOrEqualSparqlOp,
    "geq",
    [Ordering::Equal, Ordering::Greater]
);
create_binary_cmp_udf!(LessThanSparqlOp, "lt", [Ordering::Less]);
create_binary_cmp_udf!(LessOrEqualSparqlOp, "leq", [Ordering::Less, Ordering::Equal]);

#[cfg(test)]
mod tests {
    use crate::comparison::generic::LessThanSparqlOp;
    use crate::BinaryTermValueOp;
    use graphfusion_model::Numeric;
    use graphfusion_model::TermValueRef;

    #[test]
    fn test_lth_int_with_float() {
        let less_than = LessThanSparqlOp::new();
        let result = less_than
            .evaluate(
                TermValueRef::NumericLiteral(Numeric::Int(5.into())),
                TermValueRef::NumericLiteral(Numeric::Float(10.0.into())),
            )
            .unwrap();
        assert_eq!(result, true.into());
    }
}
