use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use crate::encoded::dispatch::EncRdfTerm;

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
    type Arg<'data> = EncRdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let result = match value {
            EncRdfTerm::NamedNode(_) => false,
            EncRdfTerm::BlankNode(_) => true,
            EncRdfTerm::Boolean(_) => false,
            EncRdfTerm::Numeric(_) => false,
            EncRdfTerm::SimpleLiteral(_) => false,
            EncRdfTerm::LanguageString(_) => false,
            EncRdfTerm::TypedLiteral(_) => false,
        };
        collector.append_boolean(result)?;
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
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
