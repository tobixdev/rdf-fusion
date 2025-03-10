use crate::datatypes::{RdfTerm, XsdDecimal, XsdDouble, XsdFloat, XsdInt, XsdInteger, XsdNumeric};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::EncTerm;
use crate::DFResult;
use datafusion::arrow::array::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncEffectiveBooleanValue {
    signature: Signature,
}

impl EncEffectiveBooleanValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncEffectiveBooleanValue {
    type Arg<'data> = RdfTerm<'data>;
    type Collector = BooleanBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        // TODO implement all rules
        let result = match value {
            RdfTerm::NamedNode(value) => !value.name.is_empty(),
            RdfTerm::BlankNode(value) => !value.id.is_empty(),
            RdfTerm::Boolean(value) => value.as_bool(),
            RdfTerm::Numeric(value) => match value {
                XsdNumeric::Int(value) => value != XsdInt::from(0),
                XsdNumeric::Integer(value) => value != XsdInteger::from(0),
                XsdNumeric::Float(value) => value != XsdFloat::from(0f32),
                XsdNumeric::Double(value) => value != XsdDouble::from(0f64),
                XsdNumeric::Decimal(value) => value != XsdDecimal::from(0),
            },
            RdfTerm::SimpleLiteral(value) => !value.is_empty(),
            RdfTerm::LanguageString(value) => !value.is_empty(),
            RdfTerm::TypedLiteral(value) => !value.is_empty(),
        };
        collector.append_value(result);
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_value(false);
        Ok(())
    }
}

impl ScalarUDFImpl for EncEffectiveBooleanValue {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_effective_boolean_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        dispatch_unary::<EncEffectiveBooleanValue>(self, args, number_rows)
    }
}
