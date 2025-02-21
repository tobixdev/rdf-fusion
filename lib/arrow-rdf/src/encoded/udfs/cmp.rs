use crate::encoded::dispatch::EncRdfTerm;
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
            type ArgLhs<'lhs> = EncRdfTerm<'lhs>;
            type ArgRhs<'rhs> = EncRdfTerm<'rhs>;
            type Collector = EncRdfTermBuilder;

            fn evaluate(
                collector: &mut Self::Collector,
                lhs: &Self::ArgLhs<'_>,
                rhs: &Self::ArgRhs<'_>,
            ) -> DFResult<()> {
                let result = match (lhs, rhs) {
                    (EncRdfTerm::NamedNode(l), EncRdfTerm::NamedNode(r)) => l $OP r,
                    (EncRdfTerm::BlankNode(l), EncRdfTerm::BlankNode(r)) => l $OP r,
                    (EncRdfTerm::Boolean(l), EncRdfTerm::Boolean(r)) => l $OP r,
                    (EncRdfTerm::Numeric(l), EncRdfTerm::Numeric(r)) => l $OP r,
                    (EncRdfTerm::SimpleLiteral(l), EncRdfTerm::SimpleLiteral(r)) => l $OP r,
                    (EncRdfTerm::LanguageString(l), EncRdfTerm::LanguageString(r)) => l $OP r,
                    (EncRdfTerm::TypedLiteral(l), EncRdfTerm::TypedLiteral(r)) => l $OP r,
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

create_binary_cmp_udf!(EncEq, "enc_eq", ==); // TODO
create_binary_cmp_udf!(EncGreaterThan, "enc_greater_than", >);
create_binary_cmp_udf!(EncGreaterOrEqual, "enc_greater_or_equal", >=);
create_binary_cmp_udf!(EncLessThan, "enc_less_than", <);
create_binary_cmp_udf!(EncLessOrEqual, "enc_less_or_equal", <=);

#[derive(Debug)]
pub struct EncSameTerm {
    signature: Signature,
}

impl EncSameTerm {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(), EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncSameTerm {
    type ArgLhs<'lhs> = EncRdfTerm<'lhs>;
    type ArgRhs<'rhs> = EncRdfTerm<'rhs>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let result = match (lhs, rhs) {
            (EncRdfTerm::NamedNode(l), EncRdfTerm::NamedNode(r)) => l == r,
            (EncRdfTerm::BlankNode(l), EncRdfTerm::BlankNode(r)) => l == r,
            (EncRdfTerm::Boolean(l), EncRdfTerm::Boolean(r)) => l == r,
            (EncRdfTerm::Numeric(l), EncRdfTerm::Numeric(r)) => l == r,
            (EncRdfTerm::SimpleLiteral(l), EncRdfTerm::SimpleLiteral(r)) => l == r,
            (EncRdfTerm::LanguageString(l), EncRdfTerm::LanguageString(r)) => l == r,
            (EncRdfTerm::TypedLiteral(l), EncRdfTerm::TypedLiteral(r)) => l == r,
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

impl ScalarUDFImpl for EncSameTerm {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_same_term"
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
        // Maybe we can improve this as we don't need the casting for same term (I think)
        dispatch_binary::<Self>(args, number_rows)
    }
}

#[cfg(test)]
mod tests {
    use crate::encoded::dispatch::{EncBoolean, EncRdfValue};
    use crate::encoded::scalars::encode_scalar_object;
    use crate::encoded::{EncTermField, ENC_LESS_THAN};
    use crate::{as_enc_term_array, DFResult};
    use datafusion::logical_expr::ColumnarValue;
    use oxrdf::{Literal, Term};

    #[test]
    fn test_lth_int_with_float() -> DFResult<()> {
        let lhs = encode_scalar_object(Term::from(Literal::from(5)).as_ref())?;
        let rhs = encode_scalar_object(Term::from(Literal::from(10f32)).as_ref())?;
        let result =
            ENC_LESS_THAN.invoke_batch(&[ColumnarValue::from(lhs), ColumnarValue::from(rhs)], 1)?;

        // TODO: Improve test infrastructure
        let result = match result {
            ColumnarValue::Array(arr) => {
                EncBoolean::from_array(as_enc_term_array(arr.as_ref())?, EncTermField::Boolean, 0)
            }
            ColumnarValue::Scalar(scalar) => EncBoolean::from_scalar(&scalar),
        }?;
        assert_eq!(result.0, true);
        Ok(())
    }
}
