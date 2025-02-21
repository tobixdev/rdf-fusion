use crate::encoded::dispatch::EncRdfValue;
use crate::result_collector::ResultCollector;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;
use crate::encoded::EncTermField;

pub(crate) trait EncScalarUnaryUdf {
    type Arg<'data>: EncRdfValue<'data>;
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

    match TUdf::Arg::from_scalar(&value) {
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
    let mut collector = TUdf::Collector::new();

    let type_offset_paris = values
        .type_ids()
        .iter()
        .map(|tid| EncTermField::try_from(*tid).unwrap())
        .zip(values.offsets().expect("Always Dense"));
    for (field, offset) in type_offset_paris {
        match TUdf::Arg::from_array(values, field, *offset as usize) {
            Ok(value) => TUdf::evaluate(udf, &mut collector, value)?,
            Err(_) => TUdf::evaluate_error(udf, &mut collector)?,
        }
    }

    collector.finish_columnar_value()
}
