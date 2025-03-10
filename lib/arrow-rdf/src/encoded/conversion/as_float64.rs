use crate::datatypes::{RdfTerm, XsdDouble, XsdNumeric};
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
pub struct EncAsFloat64 {
    signature: Signature,
}

impl EncAsFloat64 {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncAsFloat64 {
    type Arg<'data> = RdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let converted = match value {
            RdfTerm::Boolean(v) => XsdDouble::from(v),
            RdfTerm::Numeric(numeric) => match numeric {
                XsdNumeric::Int(v) => XsdDouble::from(v),
                XsdNumeric::Integer(v) => XsdDouble::from(v),
                XsdNumeric::Float(v) => XsdDouble::from(v),
                XsdNumeric::Double(v) => v,
                XsdNumeric::Decimal(v) => XsdDouble::from(v),
            },
            _ => {
                collector.append_null()?;
                return Ok(());
            }
        };

        collector.append_float64(converted)?;
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncAsFloat64 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_as_float64"
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
