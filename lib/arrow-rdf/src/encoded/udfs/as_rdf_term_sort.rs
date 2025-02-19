use crate::encoded::udfs::unary_dispatch::{dispatch_unary, EncScalarUnaryUdf};
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

    fn eval_named_node(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        collector.append_iri(value);
        Ok(())
    }

    fn eval_blank_node(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        collector.append_blank_node(value);
        Ok(())
    }

    fn eval_numeric_i32(collector: &mut Self::Collector, value: i32) -> DFResult<()> {
        collector.append_numeric(value as f64);
        Ok(())
    }

    fn eval_numeric_i64(collector: &mut Self::Collector, value: i64) -> DFResult<()> {
        collector.append_numeric(value as f64);
        Ok(())
    }

    fn eval_numeric_f32(collector: &mut Self::Collector, value: f32) -> DFResult<()> {
        collector.append_numeric(value as f64);
        Ok(())
    }

    fn eval_numeric_f64(collector: &mut Self::Collector, value: f64) -> DFResult<()> {
        collector.append_numeric(value);
        Ok(())
    }

    fn eval_numeric_decimal(collector: &mut Self::Collector, value: i128) -> DFResult<()> {
        // TODO #1
        let formatted =
            Decimal128Type::format_decimal(value, RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE);
        let f64 = f64::from_str(&formatted).expect("TODO decimal to f64 conversion");
        collector.append_numeric(f64);
        Ok(())
    }

    fn eval_boolean(collector: &mut Self::Collector, value: bool) -> DFResult<()> {
        collector.append_numeric(value.into());
        Ok(())
    }

    fn eval_string(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        // TODO language
        collector.append_string(value);
        Ok(())
    }

    fn eval_simple_literal(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        // TODO
        collector.append_string(value);
        Ok(())
    }

    fn eval_typed_literal(
        collector: &mut Self::Collector,
        value: &str,
        value_type: &str,
    ) -> DFResult<()> {
        // TODO
        collector.append_string(value);
        Ok(())
    }

    fn eval_null(collector: &mut Self::Collector) -> DFResult<()> {
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
        dispatch_unary::<EncAsRdfTermSort>(args, number_rows)
    }
}
