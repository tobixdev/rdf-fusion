use crate::datatypes::{XsdNumeric, XsdNumericPair};
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncDiv {
    signature: Signature,
}

impl EncDiv {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(), EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncDiv {
    type ArgLhs<'lhs> = XsdNumeric;
    type ArgRhs<'rhs> = XsdNumeric;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        match XsdNumericPair::with_casts_from(lhs, rhs) {
            XsdNumericPair::Int(lhs, rhs) => match lhs.checked_div(rhs) {
                None => collector.append_null()?,
                Some(value) => collector.append_int(value)?,
            },
            XsdNumericPair::Integer(lhs, rhs) => match lhs.checked_div(rhs) {
                None => collector.append_null()?,
                Some(value) => collector.append_integer(value)?,
            },
            XsdNumericPair::Float(lhs, rhs) => collector.append_float32(lhs / rhs)?,
            XsdNumericPair::Double(lhs, rhs) => collector.append_float64(lhs / rhs)?,
            XsdNumericPair::Decimal(lhs, rhs) => match lhs.checked_div(rhs) {
                None => collector.append_null()?,
                Some(value) => collector.append_decimal(value)?,
            },
        };
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncDiv {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_div"
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
        dispatch_binary::<EncDiv>(args, number_rows)
    }
}
