use crate::datatypes::{RdfTerm, XsdNumeric};
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
    type Arg<'data> = RdfTerm<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let datatype = match value {
            RdfTerm::NamedNode(_) => None,
            RdfTerm::BlankNode(_) => None,
            RdfTerm::SimpleLiteral(_) => Some(xsd::STRING.as_str()),
            RdfTerm::Numeric(value) => Some(match value {
                XsdNumeric::Int(_) => xsd::INT.as_str(),
                XsdNumeric::Integer(_) => xsd::INTEGER.as_str(),
                XsdNumeric::Float(_) => xsd::FLOAT.as_str(),
                XsdNumeric::Double(_) => xsd::DOUBLE.as_str(),
                XsdNumeric::Decimal(_) => xsd::DECIMAL.as_str(),
            }),
            RdfTerm::Boolean(_) => Some(xsd::BOOLEAN.as_str()),
            RdfTerm::LanguageString(_) => Some(rdf::LANG_STRING.as_str()),
            RdfTerm::TypedLiteral(value) => Some(value.literal_type),
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
