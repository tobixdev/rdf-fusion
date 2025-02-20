use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use oxiri::Iri;
use std::any::Any;

#[derive(Debug)]
pub struct EncIri {
    base_iri: Option<Iri<String>>,
    signature: Signature,
}

impl EncIri {
    pub fn new(base_iri: Option<Iri<String>>) -> Self {
        Self {
            base_iri,
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncIri {
    type Collector = EncRdfTermBuilder;

    fn eval_named_node(&self, collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        collector.append_named_node(value)?;
        Ok(())
    }

    fn eval_blank_node(&self, collector: &mut Self::Collector, _value: &str) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_numeric_i32(&self, collector: &mut Self::Collector, _value: i32) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_numeric_i64(&self, collector: &mut Self::Collector, _value: i64) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_numeric_f32(&self, collector: &mut Self::Collector, _value: f32) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_numeric_f64(&self, collector: &mut Self::Collector, _value: f64) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_numeric_decimal(&self, collector: &mut Self::Collector, _value: i128) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_boolean(&self, collector: &mut Self::Collector, _value: bool) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }

    fn eval_string(
        &self,
        collector: &mut Self::Collector,
        value: &str,
        lang: Option<&str>,
    ) -> DFResult<()> {
        if lang.is_some() {
            // https://www.w3.org/TR/sparql11-query/#func-iri
            // Passing any RDF term other than a simple literal, xsd:string or an IRI is an error.
            collector.append_null()?;
            return Ok(());
        }

        let resolving_result = if let Some(base_iri) = &self.base_iri {
            base_iri.resolve(value)
        } else {
            Iri::parse(value.to_string())
        };
        match resolving_result {
            Ok(resolving_result) => collector.append_named_node(resolving_result.as_str())?,
            Err(_) => collector.append_null()?,
        }

        Ok(())
    }

    fn eval_typed_literal(
        &self,
        collector: &mut Self::Collector,
        _value: &str,
        _value_type: &str,
    ) -> DFResult<()> {
        collector.append_string("", None)?;
        Ok(())
    }

    fn eval_null(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncIri {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_lang"
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
