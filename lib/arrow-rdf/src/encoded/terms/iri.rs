use crate::encoded::dispatch::EncRdfTerm;
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
    type Arg<'data> = EncRdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        match value {
            EncRdfTerm::NamedNode(value) => collector.append_named_node(value.0),
            EncRdfTerm::SimpleLiteral(value) => {
                let resolving_result = if let Some(base_iri) = &self.base_iri {
                    base_iri.resolve(value.0)
                } else {
                    Iri::parse(value.0.to_string())
                };
                match resolving_result {
                    Ok(resolving_result) => collector.append_named_node(resolving_result.as_str()),
                    Err(_) => collector.append_null(),
                }
            }
            _ => collector.append_null(),
        }?;
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
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
