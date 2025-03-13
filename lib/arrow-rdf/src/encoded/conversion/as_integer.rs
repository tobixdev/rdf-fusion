use crate::datatypes::{RdfTerm, XsdInteger, XsdNumeric};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncAsInteger {
    signature: Signature,
}

impl EncAsInteger {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncAsInteger {
    type Arg<'data> = RdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let converted = match value {
            RdfTerm::Boolean(v) => Some(XsdInteger::from(v)),
            RdfTerm::SimpleLiteral(v) => v
                .value
                .parse()
                .ok(),
            RdfTerm::Numeric(numeric) => match numeric {
                XsdNumeric::Int(v) => Some(XsdInteger::from(v)),
                XsdNumeric::Integer(v) => Some(v),
                XsdNumeric::Float(v) => XsdInteger::try_from(v).ok(),
                XsdNumeric::Double(v) => XsdInteger::try_from(v).ok(),
                XsdNumeric::Decimal(v) => XsdInteger::try_from(v).ok(),
            },
            _ => {
                collector.append_null()?;
                return Ok(());
            }
        };

        match converted {
            Some(converted) => collector.append_integer(converted)?,
            None => collector.append_null()?,
        };
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncAsInteger {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_as_integer"
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
