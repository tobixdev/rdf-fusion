use crate::datatypes::{RdfTerm, XsdFloat, XsdNumeric};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncAsFloat {
    signature: Signature,
}

impl EncAsFloat {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncAsFloat {
    type Arg<'data> = RdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let converted = match value {
            RdfTerm::Boolean(v) => Some(XsdFloat::from(v)),
            RdfTerm::SimpleLiteral(v) => v.value.parse().ok(),
            RdfTerm::Numeric(numeric) => Some(match numeric {
                XsdNumeric::Int(v) => XsdFloat::from(v),
                XsdNumeric::Integer(v) => XsdFloat::from(v),
                XsdNumeric::Float(v) => v,
                XsdNumeric::Double(v) => XsdFloat::from(v),
                XsdNumeric::Decimal(v) => XsdFloat::from(v),
            }),
            _ => {
                collector.append_null()?;
                return Ok(());
            }
        };

        match converted {
            None => collector.append_null()?,
            Some(converted) => collector.append_float(converted)?,
        }
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncAsFloat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_as_float32"
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

#[cfg(test)]
mod tests {
    use crate::datatypes::{RdfTerm, RdfValue, XsdFloat, XsdInt, XsdNumeric};
    use crate::encoded::conversion::as_float::EncAsFloat;
    use crate::encoded::dispatch_unary::EncScalarUnaryUdf;
    use crate::encoded::EncRdfTermBuilder;
    use crate::{as_enc_term_array, DFResult};

    #[test]
    fn test_enc_as_float() -> DFResult<()> {
        let udf = EncAsFloat::new();
        let mut collector = EncRdfTermBuilder::new();

        udf.evaluate(
            &mut collector,
            RdfTerm::Numeric(XsdNumeric::Int(XsdInt::from(10))),
        )?;

        let array = collector.finish()?;
        let result = as_enc_term_array(array.as_ref())?;
        let result = XsdNumeric::from_enc_array(result, 0)?;

        assert_eq!(result, XsdNumeric::Float(XsdFloat::from(10.0)));
        Ok(())
    }
}
