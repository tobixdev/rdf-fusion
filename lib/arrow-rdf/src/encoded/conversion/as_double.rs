use crate::datatypes::{RdfTerm, XsdDouble, XsdNumeric};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncAsDouble {
    signature: Signature,
}

impl EncAsDouble {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncAsDouble {
    type Arg<'data> = RdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let converted = match value {
            RdfTerm::Boolean(v) => Some(XsdDouble::from(v)),
            RdfTerm::SimpleLiteral(v) => v
                .value
                .parse()
                .ok(),
            RdfTerm::Numeric(numeric) => Some(match numeric {
                XsdNumeric::Int(v) => XsdDouble::from(v),
                XsdNumeric::Integer(v) => XsdDouble::from(v),
                XsdNumeric::Float(v) => XsdDouble::from(v),
                XsdNumeric::Double(v) => v,
                XsdNumeric::Decimal(v) => XsdDouble::from(v),
            }),
            _ => {
                collector.append_null()?;
                return Ok(());
            }
        };

        match converted {
            None => collector.append_null()?,
            Some(converted) => collector.append_float64(converted)?,
        }
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncAsDouble {
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
