use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use oxrdf::vocab::xsd;
use std::any::Any;
use std::collections::HashSet;

#[derive(Debug)]
pub struct EncIsNumeric {
    signature: Signature,
}

impl EncIsNumeric {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncIsNumeric {
    type Collector = EncRdfTermBuilder;

    fn eval_named_node(&self, collector: &mut Self::Collector, _value: &str) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_blank_node(&self, collector: &mut Self::Collector, _value: &str) -> DFResult<()> {
        collector.append_boolean(false)?;
        Ok(())
    }

    fn eval_numeric_i32(&self, collector: &mut Self::Collector, _value: i32) -> DFResult<()> {
        collector.append_boolean(true)?;
        Ok(())
    }

    fn eval_numeric_i64(&self, collector: &mut Self::Collector, _value: i64) -> DFResult<()> {
        collector.append_boolean(true)?;
        Ok(())
    }

    fn eval_numeric_f32(&self, collector: &mut Self::Collector, _value: f32) -> DFResult<()> {
        collector.append_boolean(true)?;
        Ok(())
    }

    fn eval_numeric_f64(&self, collector: &mut Self::Collector, _value: f64) -> DFResult<()> {
        collector.append_boolean(true)?;
        Ok(())
    }

    fn eval_numeric_decimal(&self, collector: &mut Self::Collector, _value: i128) -> DFResult<()> {
        collector.append_boolean(true)?;
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
        value_type: &str,
    ) -> DFResult<()> {
        let numeric_types = HashSet::from([
          xsd::INTEGER.as_str(),
          xsd::DECIMAL.as_str(),
          xsd::FLOAT.as_str(),
          xsd::DOUBLE.as_str(),
          xsd::STRING.as_str(),
          xsd::BOOLEAN.as_str(),
          xsd::DATE_TIME.as_str(),
          xsd::NON_POSITIVE_INTEGER.as_str(),
          xsd::NEGATIVE_INTEGER.as_str(),
          xsd::LONG.as_str(),
          xsd::INT.as_str(),
          xsd::SHORT.as_str(),
          xsd::BYTE.as_str(),
          xsd::NON_NEGATIVE_INTEGER.as_str(),
          xsd::UNSIGNED_LONG.as_str(),
          xsd::UNSIGNED_INT.as_str(),
          xsd::UNSIGNED_SHORT.as_str(),
          xsd::UNSIGNED_BYTE.as_str(),
          xsd::POSITIVE_INTEGER.as_str()
        ]);

        // TODO: We must check whether the literal is valid or encode all numeric types in the union

        collector.append_boolean(numeric_types.contains(value_type))?;
        Ok(())
    }

    fn eval_null(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncIsNumeric {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_is_numeric"
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
