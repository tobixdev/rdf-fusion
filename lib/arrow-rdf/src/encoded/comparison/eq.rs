use crate::datatypes::RdfTerm;
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct EncEq {
    signature: Signature,
}

impl EncEq {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(), EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncEq {
    type ArgLhs<'lhs> = RdfTerm<'lhs>;
    type ArgRhs<'rhs> = RdfTerm<'rhs>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let result = match (lhs, rhs) {
            // Same term are also equal.
            (RdfTerm::TypedLiteral(l), RdfTerm::TypedLiteral(r)) if l == r => Some(true),
            // Cannot say anything about unsupported typed literals that are not the same term.
            (RdfTerm::TypedLiteral(_), _) => None,
            (_, RdfTerm::TypedLiteral(_)) => None,
            // For numerics, compare values.
            (RdfTerm::Numeric(lhs), RdfTerm::Numeric(rhs)) => Some(lhs.cmp(rhs) == Ordering::Equal),
            // Otherwise compare for equality.
            _ => Some(lhs == rhs),
        };

        match result {
            Some(result) => collector.append_boolean(result)?,
            None => collector.append_null()?,
        }

        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncEq {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_eq"
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
