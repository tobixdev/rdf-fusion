use crate::datatypes::XsdNumeric;
use crate::encoded::dispatch_binary::EncScalarBinaryUdf;
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncFloor {
    signature: Signature,
}

impl EncFloor {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncFloor {
    type Arg<'data> = XsdNumeric;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        match value {
            XsdNumeric::Int(value) => collector.append_int(value)?,
            XsdNumeric::Integer(value) => collector.append_integer(value)?,
            XsdNumeric::Float(value) => collector.append_float32(value.floor())?,
            XsdNumeric::Double(value) => collector.append_float64(value.floor())?,
            XsdNumeric::Decimal(value) => match value.checked_floor() {
                Some(value) => collector.append_decimal(value)?,
                None => collector.append_null()?,
            },
        }
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncFloor {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_ceil"
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
