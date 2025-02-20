use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use oxrdf::vocab::{rdf, xsd};
use std::any::Any;

#[derive(Debug)]
pub struct EncDatatype {
    signature: Signature,
}

impl EncDatatype {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncDatatype {
    type Collector = EncRdfTermBuilder;

    fn eval_named_node(&self, collector: &mut Self::Collector, _value: &str) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_blank_node(&self, collector: &mut Self::Collector, _value: &str) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_numeric_i32(&self, collector: &mut Self::Collector, _value: i32) -> DFResult<()> {
        collector.append_string(xsd::INT.as_str(), None)?;
        Ok(())
    }

    fn eval_numeric_i64(&self, collector: &mut Self::Collector, _value: i64) -> DFResult<()> {
        collector.append_string(xsd::INTEGER.as_str(), None)?;
        Ok(())
    }

    fn eval_numeric_f32(&self, collector: &mut Self::Collector, _value: f32) -> DFResult<()> {
        collector.append_string(xsd::FLOAT.as_str(), None)?;
        Ok(())
    }

    fn eval_numeric_f64(&self, collector: &mut Self::Collector, _value: f64) -> DFResult<()> {
        collector.append_string(xsd::DOUBLE.as_str(), None)?;
        Ok(())
    }

    fn eval_numeric_decimal(&self, collector: &mut Self::Collector, _value: i128) -> DFResult<()> {
        collector.append_string(xsd::DECIMAL.as_str(), None)?;
        Ok(())
    }

    fn eval_boolean(&self, collector: &mut Self::Collector, _value: bool) -> DFResult<()> {
        collector.append_string(xsd::BOOLEAN.as_str(), None)?;
        Ok(())
    }

    fn eval_string(
        &self,
        collector: &mut Self::Collector,
        _value: &str,
        lang: Option<&str>,
    ) -> DFResult<()> {
        match lang {
            None => collector.append_string(xsd::STRING.as_str(), None)?,
            Some(_) => collector.append_string(rdf::LANG_STRING.as_str(), None)?,
        }
        Ok(())
    }

    fn eval_typed_literal(
        &self,
        collector: &mut Self::Collector,
        _value: &str,
        value_type: &str,
    ) -> DFResult<()> {
        collector.append_string(value_type, None)?;
        Ok(())
    }

    fn eval_null(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncDatatype {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_datatype"
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
