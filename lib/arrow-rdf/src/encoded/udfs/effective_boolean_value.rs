use crate::encoded::dispatch::{EncNumeric, EncRdfTerm};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use datafusion::arrow::array::BooleanBuilder;

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
    type Arg<'data> = EncRdfTerm<'data>;
    type Collector = BooleanBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        // TODO implement all rules
        let result = match value {
            EncRdfTerm::NamedNode(value) => !value.0.is_empty(),
            EncRdfTerm::BlankNode(value) => !value.0.is_empty(),
            EncRdfTerm::Boolean(value) => value.0,
            EncRdfTerm::Numeric(value) => match value {
                EncNumeric::I32(value) => value != 0,
                EncNumeric::I64(value) => value != 0,
                EncNumeric::F32(value) => value != 0f32,
                EncNumeric::F64(value) => value != 0f64,
                EncNumeric::Decimal(value) => value != 0,
            },
            EncRdfTerm::SimpleLiteral(value) => !value.is_empty(),
            EncRdfTerm::LanguageString(value) => !value.is_empty(),
            EncRdfTerm::TypedLiteral(value) => !value.is_empty(),
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
