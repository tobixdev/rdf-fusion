use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
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
    type Collector = EncRdfTermBuilder;

    fn eval_named_node(&self, collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        collector.append_boolean(!value.is_empty())?;
        Ok(())
    }

    fn eval_blank_node(&self, collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        collector.append_boolean(!value.is_empty())?;
        Ok(())
    }

    fn eval_numeric_i32(&self, collector: &mut Self::Collector, value: i32) -> DFResult<()> {
        collector.append_boolean(value != 0)?;
        Ok(())
    }

    fn eval_numeric_i64(&self, collector: &mut Self::Collector, value: i64) -> DFResult<()> {
        collector.append_boolean(value != 0)?;
        Ok(())
    }

    fn eval_numeric_f32(&self, collector: &mut Self::Collector, value: f32) -> DFResult<()> {
        collector.append_boolean(!value.is_nan() && value != 0.0_f32)?;
        Ok(())
    }

    fn eval_numeric_f64(&self, collector: &mut Self::Collector, value: f64) -> DFResult<()> {
        collector.append_boolean(value.is_nan() && value != 0.0_f64)?;
        Ok(())
    }

    fn eval_numeric_decimal(&self, collector: &mut Self::Collector, value: i128) -> DFResult<()> {
        collector.append_boolean(value != 0)?;
        Ok(())
    }

    fn eval_boolean(&self, collector: &mut Self::Collector, value: bool) -> DFResult<()> {
        collector.append_boolean(value)?;
        Ok(())
    }

    fn eval_string(
        &self,
        collector: &mut Self::Collector,
        value: &str,
        _lang: Option<&str>,
    ) -> DFResult<()> {
        collector.append_boolean(!value.is_empty())?;
        Ok(())
    }

    fn eval_typed_literal(
        &self,
        collector: &mut Self::Collector,
        value: &str,
        _value_type: &str,
    ) -> DFResult<()> {
        collector.append_boolean(!value.is_empty())?;
        Ok(())
    }

    fn eval_null(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_boolean(false)?;
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
        Ok(EncTerm::term_type())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        dispatch_unary::<EncEffectiveBooleanValue>(self, args, number_rows)
    }
}
