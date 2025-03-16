use crate::datatypes::{RdfTerm, XsdBoolean, XsdNumeric};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncAsBoolean {
    signature: Signature,
}

impl EncAsBoolean {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncAsBoolean {
    type Arg<'data> = RdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let converted = match value {
            RdfTerm::Boolean(v) => Some(XsdBoolean::from(v)),
            RdfTerm::SimpleLiteral(v) => v.value.parse().ok(),
            RdfTerm::Numeric(numeric) => Some(match numeric {
                XsdNumeric::Int(v) => XsdBoolean::from(v),
                XsdNumeric::Integer(v) => XsdBoolean::from(v),
                XsdNumeric::Float(v) => XsdBoolean::from(v),
                XsdNumeric::Double(v) => XsdBoolean::from(v),
                XsdNumeric::Decimal(v) => XsdBoolean::from(v),
            }),
            _ => None,
        };

        match converted {
            None => collector.append_null()?,
            Some(converted) => collector.append_boolean(converted.as_bool())?,
        }
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncAsBoolean {
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
