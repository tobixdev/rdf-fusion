use crate::datatypes::RdfTerm;
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

macro_rules! create_binary_cmp_udf {
    ($STRUCT: ident, $NAME: expr, $OP: tt) => {
        #[derive(Debug)]
        pub struct $STRUCT {
            signature: Signature,
        }

        impl $STRUCT {
            pub fn new() -> Self {
                Self {
                    signature: Signature::new(
                        TypeSignature::Exact(vec![EncTerm::term_type(), EncTerm::term_type()]),
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl EncScalarBinaryUdf for $STRUCT {
            type ArgLhs<'lhs> = RdfTerm<'lhs>;
            type ArgRhs<'rhs> = RdfTerm<'rhs>;
            type Collector = EncRdfTermBuilder;

            fn evaluate(
                collector: &mut Self::Collector,
                lhs: &Self::ArgLhs<'_>,
                rhs: &Self::ArgRhs<'_>,
            ) -> DFResult<()> {
                let result = match (lhs, rhs) {
                    (RdfTerm::NamedNode(l), RdfTerm::NamedNode(r)) => l $OP r,
                    (RdfTerm::BlankNode(l), RdfTerm::BlankNode(r)) => l $OP r,
                    (RdfTerm::Boolean(l), RdfTerm::Boolean(r)) => l $OP r,
                    (RdfTerm::Numeric(l), RdfTerm::Numeric(r)) => l $OP r,
                    (RdfTerm::SimpleLiteral(l), RdfTerm::SimpleLiteral(r)) => l $OP r,
                    (RdfTerm::LanguageString(l), RdfTerm::LanguageString(r)) => l $OP r,
                    (RdfTerm::TypedLiteral(l), RdfTerm::TypedLiteral(r)) => l $OP r,
                    _ => false,
                };
                collector.append_boolean(result)?;
                Ok(())
            }

            fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
                collector.append_null()?;
                Ok(())
            }
        }

        impl ScalarUDFImpl for $STRUCT {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $NAME
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok(EncTerm::term_type())
            }

            fn invoke_batch(
                &self,
                args: &[ColumnarValue],
                number_rows: usize,
            ) -> datafusion::common::Result<ColumnarValue> {
                dispatch_binary::<Self>(args, number_rows)
            }
        }
    };
}

create_binary_cmp_udf!(EncGreaterThan, "enc_greater_than", >);
create_binary_cmp_udf!(EncGreaterOrEqual, "enc_greater_or_equal", >=);
create_binary_cmp_udf!(EncLessThan, "enc_less_than", <);
create_binary_cmp_udf!(EncLessOrEqual, "enc_less_or_equal", <=);

#[cfg(test)]
mod tests {
    use crate::datatypes::{RdfValue, XsdBoolean};
    use crate::encoded::scalars::encode_scalar_object;
    use crate::encoded::ENC_LESS_THAN;
    use crate::{as_enc_term_array, DFResult};
    use datafusion::logical_expr::ColumnarValue;
    use oxrdf::{Literal, Term};

    #[test]
    fn test_lth_int_with_float() -> DFResult<()> {
        let lhs = encode_scalar_object(Term::from(Literal::from(5)).as_ref())?;
        let rhs = encode_scalar_object(Term::from(Literal::from(10f32)).as_ref())?;
        let result =
            ENC_LESS_THAN.invoke_batch(&[ColumnarValue::from(lhs), ColumnarValue::from(rhs)], 1)?;

        match result {
            ColumnarValue::Array(arr) => {
                let term_array = as_enc_term_array(arr.as_ref())?;
                let value = XsdBoolean::from_enc_array(term_array, 0)?;
                assert!(value.as_bool());
            }
            ColumnarValue::Scalar(scalar) => {
                let value = XsdBoolean::from_enc_scalar(&scalar)?;
                assert!(value.as_bool());
            }
        };

        Ok(())
    }
}
