use crate::encoded::dispatch::{EncNumeric, EncNumericPair};
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
    type ArgLhs<'lhs> = EncNumeric;
    type ArgRhs<'rhs> = EncNumeric;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        match EncNumericPair::with_casts_from(lhs, rhs) {
            EncNumericPair::I32(lhs, rhs) => match lhs.checked_div(rhs) {
                None => collector.append_null()?,
                Some(value) => collector.append_int(value)?,
            },
            EncNumericPair::I64(lhs, rhs) => match lhs.checked_div(rhs) {
                None => collector.append_null()?,
                Some(value) => collector.append_integer(value)?,
            },
            EncNumericPair::F32(lhs, rhs) => collector.append_float32(lhs / rhs)?,
            EncNumericPair::F64(lhs, rhs) => collector.append_float64(lhs / rhs)?,
            EncNumericPair::Decimal(lhs, rhs) => todo!("Decimal"),
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
