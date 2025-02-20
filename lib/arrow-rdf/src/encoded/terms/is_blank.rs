use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use datafusion::common::exec_err;

#[derive(Debug)]
pub struct EncIsBlank {
    signature: Signature,
}

impl EncIsBlank {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncIsBlank {
    type Collector = EncRdfTermBuilder;

    fn eval_named_node(&self, collector: &mut Self::Collector, _value: &str) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_blank_node(&self, collector: &mut Self::Collector, _value: &str) -> DFResult<()> {
        collector.append_boolean(true)?;
        Ok(())
    }

    fn eval_numeric_i32(&self, collector: &mut Self::Collector, _value: i32) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_numeric_i64(&self, collector: &mut Self::Collector, _value: i64) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_numeric_f32(&self, collector: &mut Self::Collector, _value: f32) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_numeric_f64(&self, collector: &mut Self::Collector, _value: f64) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_numeric_decimal(&self, collector: &mut Self::Collector, _value: i128) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_boolean(&self, collector: &mut Self::Collector, _value: bool) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_string(&self, collector: &mut Self::Collector, _value: &str, _lang: Option<&str>) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_typed_literal(
        &self,
        collector: &mut Self::Collector,
        _value: &str,
        _value_type: &str,
    ) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_null(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncIsBlank {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_is_blank"
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
