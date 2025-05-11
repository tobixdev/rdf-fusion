use crate::ThinResult;
use crate::{BinarySparqlOp, SparqlOp};
use graphfusion_model::ThinError;
use graphfusion_model::{Boolean, TypedValueRef};
use std::cmp::Ordering;

macro_rules! create_binary_cmp_udf {
    ($STRUCT: ident, $ORDERINGS: expr) => {
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

        impl SparqlOp for $STRUCT {}

        impl BinarySparqlOp for $STRUCT {
            type ArgLhs<'data> = TypedValueRef<'data>;
            type ArgRhs<'data> = TypedValueRef<'data>;
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

create_binary_cmp_udf!(EqSparqlOp, [Ordering::Equal]);
create_binary_cmp_udf!(GreaterThanSparqlOp, [Ordering::Greater]);
create_binary_cmp_udf!(GreaterOrEqualSparqlOp, [Ordering::Equal, Ordering::Greater]);
create_binary_cmp_udf!(LessThanSparqlOp, [Ordering::Less]);
create_binary_cmp_udf!(LessOrEqualSparqlOp, [Ordering::Less, Ordering::Equal]);

#[cfg(test)]
mod tests {
    use crate::comparison::generic::LessThanSparqlOp;
    use crate::BinarySparqlOp;
    use graphfusion_model::Numeric;
    use graphfusion_model::TypedValueRef;

    #[test]
    fn test_lth_int_with_float() {
        let less_than = LessThanSparqlOp::new();
        let result = less_than
            .evaluate(
                TypedValueRef::NumericLiteral(Numeric::Int(5.into())),
                TypedValueRef::NumericLiteral(Numeric::Float(10.0.into())),
            )
            .unwrap();
        assert_eq!(result, true.into());
    }
}
