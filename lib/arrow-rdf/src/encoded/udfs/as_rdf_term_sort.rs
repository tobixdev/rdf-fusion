use crate::datatypes::{RdfTerm, XsdDouble, XsdNumeric};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::EncTerm;
use crate::sorting::{RdfTermSort, RdfTermSortBuilder};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncAsRdfTermSort {
    signature: Signature,
}

impl EncAsRdfTermSort {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncAsRdfTermSort {
    type Collector = RdfTermSortBuilder;
    type Arg<'data> = RdfTerm<'data>;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        match value {
            RdfTerm::NamedNode(value) => collector.append_iri(value.name),
            RdfTerm::BlankNode(value) => collector.append_blank_node(value.id),
            RdfTerm::Boolean(value) => collector.append_boolean(value.as_bool()),
            RdfTerm::Numeric(value) => collector.append_numeric(match value {
                XsdNumeric::Int(value) => XsdDouble::from(value).as_f64(),
                XsdNumeric::Integer(value) => XsdDouble::from(value).as_f64(),
                XsdNumeric::Float(value) => XsdDouble::from(value).as_f64(),
                XsdNumeric::Double(value) => value.as_f64(),
                XsdNumeric::Decimal(value) => XsdDouble::from(value).as_f64(),
            }),
            RdfTerm::SimpleLiteral(value) => collector.append_string(value.value),
            RdfTerm::LanguageString(value) => collector.append_string(value.value),
            RdfTerm::TypedLiteral(value) => collector.append_string(value.value),
        };
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null();
        Ok(())
    }
}

impl ScalarUDFImpl for EncAsRdfTermSort {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_as_rdf_term_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(RdfTermSort::data_type())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        dispatch_unary(self, args, number_rows)
    }
}
