use crate::encoded::dispatch::{EncNumeric, EncNumericPair};
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};

#[derive(Debug)]
pub struct EncUnaryPlus {
    signature: Signature,
}

impl EncUnaryPlus {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncUnaryPlus {
    type Arg<'data> = EncNumeric;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        match value {
            EncNumeric::I32(value) => collector.append_int(value)?,
            EncNumeric::I64(value) => collector.append_integer(value)?,
            EncNumeric::F32(value) => collector.append_float32(value)?,
            EncNumeric::F64(value) => collector.append_float64(value)?,
            EncNumeric::Decimal(value) => collector.append_decimal(value)?,
        }
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncUnaryPlus {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_unary_plus"
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
        dispatch_unary(self, args, number_rows)
    }
}
