use crate::encoded::dispatch::{EncNumeric, EncRdfTerm};
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::EncTerm;
use crate::sorting::{RdfTermSort, RdfTermSortBuilder};
use crate::{DFResult, RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, DecimalType};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::str::FromStr;

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
    type Arg<'data> = EncRdfTerm<'data>;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        match value {
            EncRdfTerm::NamedNode(value) => collector.append_iri(value.0),
            EncRdfTerm::BlankNode(value) => collector.append_blank_node(value.0),
            EncRdfTerm::Boolean(value) => collector.append_boolean(value.0),
            EncRdfTerm::Numeric(value) => collector.append_numeric(match value {
                EncNumeric::I32(value) => value as f64,
                EncNumeric::I64(value) => value as f64,
                EncNumeric::F32(value) => value as f64,
                EncNumeric::F64(value) => value,
                EncNumeric::Decimal(value) => {
                    // TODO #1
                    let formatted = Decimal128Type::format_decimal(
                        value,
                        RDF_DECIMAL_PRECISION,
                        RDF_DECIMAL_SCALE,
                    );
                    f64::from_str(&formatted).expect("TODO decimal to f64 conversion")
                }
            }),
            EncRdfTerm::SimpleLiteral(value) => collector.append_string(value.0),
            EncRdfTerm::LanguageString(value) => collector.append_string(value.0),
            EncRdfTerm::TypedLiteral(value) => collector.append_string(value.0),
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
