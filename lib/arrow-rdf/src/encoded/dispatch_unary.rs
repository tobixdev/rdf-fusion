use crate::datatypes::{RdfTerm, RdfValue, XsdBoolean};
use crate::encoded::EncTermField;
use crate::result_collector::ResultCollector;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

pub(crate) trait EncScalarUnaryUdf {
    type Arg<'data>: RdfValue<'data>;
    type Collector: ResultCollector;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()>;

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()>;
}

pub fn dispatch_unary<TUdf>(
    udf: &TUdf,
    args: &[ColumnarValue],
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    match args {
        [ColumnarValue::Array(lhs)] => dispatch_unary_array(udf, lhs.clone()),
        [ColumnarValue::Scalar(lhs)] => dispatch_unary_scalar(udf, lhs.clone()),
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn dispatch_unary_scalar<TUdf>(udf: &TUdf, value: ScalarValue) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    let mut collector = TUdf::Collector::new();

    match TUdf::Arg::from_enc_scalar(&value) {
        Ok(value) => TUdf::evaluate(udf, &mut collector, value)?,
        Err(_) => TUdf::evaluate_error(udf, &mut collector)?,
    }

    collector.finish_columnar_value()
}

fn dispatch_unary_array<TUdf>(udf: &TUdf, values: Arc<dyn Array>) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    let values = as_enc_term_array(&values).expect("RDF term array expected");

    let booleans = values.child(EncTermField::Boolean.type_id()).as_boolean();
    if values.len() == booleans.len() {
        let offsets = values.offsets().expect("Always dense");
        return dispatch_unary_array_boolean(udf, offsets, booleans);
    }

    let mut collector = TUdf::Collector::new();
    for i in 0..values.len() {
        match TUdf::Arg::from_enc_array(values, i) {
            Ok(value) => TUdf::evaluate(udf, &mut collector, value)?,
            Err(_) => TUdf::evaluate_error(udf, &mut collector)?,
        }
    }

    collector.finish_columnar_value()
}

#[inline(never)]
fn dispatch_unary_array_boolean<TUdf>(
    udf: &TUdf,
    offsets: &ScalarBuffer<i32>,
    values: &BooleanArray,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    let mut collector = TUdf::Collector::new();
    for offset in offsets {
        let value = values.value(*offset as usize);
        TUdf::evaluate(
            udf,
            &mut collector,
            TUdf::Arg::from_term(RdfTerm::Boolean(XsdBoolean::from(value)))?,
        )?
    }

    collector.finish_columnar_value()
}
