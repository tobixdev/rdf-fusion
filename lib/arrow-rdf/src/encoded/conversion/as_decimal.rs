use crate::datatypes::{RdfTerm, XsdDecimal, XsdNumeric};
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
pub struct EncAsDecimal {
    signature: Signature,
}

impl EncAsDecimal {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncAsDecimal {
    type Arg<'data> = RdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let converted = match value {
            RdfTerm::Boolean(v) => Ok(XsdDecimal::from(v)),
            RdfTerm::Numeric(numeric) => match numeric {
                XsdNumeric::Int(v) => Ok(XsdDecimal::from(v)),
                XsdNumeric::Integer(v) => Ok(XsdDecimal::from(v)),
                XsdNumeric::Float(v) => XsdDecimal::try_from(v),
                XsdNumeric::Double(v) => XsdDecimal::try_from(v),
                XsdNumeric::Decimal(v) => Ok(v),
            },
            _ => {
                collector.append_null()?;
                return Ok(());
            }
        };

        match converted {
            Ok(converted) => {
                collector.append_decimal(converted)?;
            }
            Err(_) => {
                collector.append_null()?;
            }
        }

        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncAsDecimal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_as_decimal"
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
