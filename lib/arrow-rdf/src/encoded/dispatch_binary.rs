use crate::encoded::dispatch::EncRdfValue;
use crate::result_collector::ResultCollector;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::common::{
    exec_err, not_impl_datafusion_err, not_impl_err, DataFusionError, ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;

pub(crate) trait EncScalarBinaryUdf {
    type ArgLhs<'lhs>: EncRdfValue<'lhs>;
    type ArgRhs<'rhs>: EncRdfValue<'rhs>;
    type Collector: ResultCollector;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()>;
    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()>;
}

pub fn dispatch_binary<TUdf>(
    args: &[ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    match args {
        [ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)] => {
            dispatch_binary_array_array::<TUdf>(lhs, rhs, number_of_rows)
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)] => {
            dispatch_binary_scalar_array::<TUdf>(lhs, rhs, number_of_rows)
        }
        [ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)] => {
            dispatch_binary_array_scalar::<TUdf>(rhs, lhs, number_of_rows)
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)] => {
            dispatch_binary_scalar_scalar::<TUdf>(lhs, rhs, number_of_rows)
        }
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn dispatch_binary_array_array<TUdf>(
    _lhs: &dyn Array,
    _rhs: &dyn Array,
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    Err(not_impl_datafusion_err!("dispatch_binary_array_array"))
}

fn dispatch_binary_scalar_array<TUdf>(
    lhs: &ScalarValue,
    _rhs: &dyn Array,
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    let ScalarValue::Union(element, _, _) = lhs else {
        return exec_err!("Unexpected type for scalar.");
    };

    match element {
        None => not_impl_err!("dispatch_binary_scalar_array: None Case"),
        Some(_) => not_impl_err!("dispatch_binary_scalar_array: Some Case"),
    }
}

fn dispatch_binary_array_scalar<TUdf>(
    lhs: &ScalarValue,
    rhs: &dyn Array,
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    let mut collector = TUdf::Collector::new();

    let lhs_value = TUdf::ArgLhs::from_scalar(lhs);
    let lhs_value = match lhs_value {
        Ok(value) => value,
        Err(_) => {
            for _ in 0..number_of_rows {
                TUdf::evaluate_error(&mut collector)?;
            }
            return collector.finish_columnar_value();
        }
    };

    let rhs = as_enc_term_array(rhs).expect("RDF term");
    for i in 0..number_of_rows {
        match TUdf::ArgRhs::from_array(rhs, i) {
            Ok(rhs_value) => TUdf::evaluate(&mut collector, &lhs_value, &rhs_value)?,
            Err(_) => TUdf::evaluate_error(&mut collector)?,
        }
    }

    collector.finish_columnar_value()
}

fn dispatch_binary_scalar_scalar<TUdf>(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    let lhs = TUdf::ArgLhs::from_scalar(lhs);
    let rhs = TUdf::ArgRhs::from_scalar(rhs);

    let mut collector = TUdf::Collector::new();

    match (lhs, rhs) {
        (Ok(lhs), Ok(rhs)) => {
            for _ in 0.._number_of_rows {
                TUdf::evaluate(&mut collector, &lhs, &rhs)?;
            }
        }
        _ => TUdf::evaluate_error(&mut collector)?,
    }

    collector.finish_columnar_value()
}
