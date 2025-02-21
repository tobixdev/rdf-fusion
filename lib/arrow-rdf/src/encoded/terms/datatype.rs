use crate::encoded::dispatch::{EncNumeric, EncRdfTerm};
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
    type Arg<'data> = EncRdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let datatype = match value {
            EncRdfTerm::NamedNode(_) => None,
            EncRdfTerm::BlankNode(_) => None,
            EncRdfTerm::SimpleLiteral(_) => Some(xsd::STRING.as_str()),
            EncRdfTerm::Numeric(value) => Some(match value {
                EncNumeric::I32(_) => xsd::INT.as_str(),
                EncNumeric::I64(_) => xsd::INTEGER.as_str(),
                EncNumeric::F32(_) => xsd::FLOAT.as_str(),
                EncNumeric::F64(_) => xsd::DOUBLE.as_str(),
                EncNumeric::Decimal(_) => xsd::DECIMAL.as_str(),
            }),
            EncRdfTerm::Boolean(_) => Some(xsd::BOOLEAN.as_str()),
            EncRdfTerm::LanguageString(_) => Some(rdf::LANG_STRING.as_str()),
            EncRdfTerm::TypedLiteral(value) => Some(value.1),
        };

        match datatype {
            None => collector.append_null(),
            Some(datatype) => collector.append_string(datatype, None),
        }?;

        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
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
