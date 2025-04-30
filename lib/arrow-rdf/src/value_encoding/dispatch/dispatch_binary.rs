use crate::value_encoding::from_encoded_term::FromEncodedTerm;
use crate::value_encoding::scalars::encode_scalar_null;
use crate::value_encoding::write_enc_term::WriteEncTerm;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use functions_scalar::ScalarBinaryRdfOp;

pub fn dispatch_binary<'data, TUdf>(
    udf: &TUdf,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarBinaryRdfOp,
    TUdf::ArgLhs<'data>: FromEncodedTerm<'data>,
    TUdf::ArgRhs<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    match args {
        [ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)] => {
            dispatch_binary_array_array(udf, lhs, rhs, number_of_rows)
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)] => {
            dispatch_binary_scalar_array(udf, lhs, rhs, number_of_rows)
        }
        [ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)] => {
            dispatch_binary_array_scalar(udf, lhs, rhs, number_of_rows)
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)] => {
            dispatch_binary_scalar_scalar(udf, lhs, rhs)
        }
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn dispatch_binary_array_array<'data, TUdf>(
    udf: &TUdf,
    lhs: &'data dyn Array,
    rhs: &'data dyn Array,
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarBinaryRdfOp,
    TUdf::ArgLhs<'data>: FromEncodedTerm<'data>,
    TUdf::ArgRhs<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    let lhs = as_enc_term_array(lhs)?;
    let rhs = as_enc_term_array(rhs)?;

    let results = (0..number_of_rows).map(|i| {
        let arg0 = TUdf::ArgLhs::from_enc_array(lhs, i);
        let arg1 = TUdf::ArgRhs::from_enc_array(rhs, i);
        match (arg0, arg1) {
            (Ok(arg0), Ok(arg1)) => udf.evaluate(arg0, arg1),
            _ => udf.evaluate_error(),
        }
    });
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

fn dispatch_binary_scalar_array<'data, TUdf>(
    udf: &TUdf,
    lhs: &'data ScalarValue,
    rhs: &'data dyn Array,
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarBinaryRdfOp,
    TUdf::ArgLhs<'data>: FromEncodedTerm<'data>,
    TUdf::ArgRhs<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    let lhs_value = TUdf::ArgLhs::from_enc_scalar(lhs);
    let Ok(lhs_value) = lhs_value else {
        let result = udf.evaluate_error();
        let result = match result {
            Ok(value) => value.into_scalar_value()?,
            Err(_) => encode_scalar_null(),
        };
        return Ok(ColumnarValue::Scalar(result));
    };

    let rhs = as_enc_term_array(rhs)?;
    let results = (0..number_of_rows).map(|i| {
        let rhs_value = TUdf::ArgRhs::from_enc_array(rhs, i);
        match rhs_value {
            Ok(rhs_value) => udf.evaluate(lhs_value, rhs_value),
            _ => udf.evaluate_error(),
        }
    });
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

fn dispatch_binary_array_scalar<'data, TUdf>(
    udf: &TUdf,
    lhs: &'data dyn Array,
    rhs: &'data ScalarValue,
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarBinaryRdfOp,
    TUdf::ArgLhs<'data>: FromEncodedTerm<'data>,
    TUdf::ArgRhs<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    let rhs_value = TUdf::ArgRhs::from_enc_scalar(rhs);
    let Ok(rhs_value) = rhs_value else {
        let result = udf.evaluate_error();
        let result = match result {
            Ok(value) => value.into_scalar_value()?,
            Err(_) => encode_scalar_null(),
        };
        return Ok(ColumnarValue::Scalar(result));
    };

    let lhs = as_enc_term_array(lhs)?;
    let results = (0..number_of_rows).map(|i| {
        let lhs_value = TUdf::ArgLhs::from_enc_array(lhs, i);
        match lhs_value {
            Ok(lhs_value) => udf.evaluate(lhs_value, rhs_value),
            _ => udf.evaluate_error(),
        }
    });
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

fn dispatch_binary_scalar_scalar<'data, TUdf>(
    udf: &TUdf,
    lhs: &'data ScalarValue,
    rhs: &'data ScalarValue,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarBinaryRdfOp,
    TUdf::ArgLhs<'data>: FromEncodedTerm<'data>,
    TUdf::ArgRhs<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    let lhs = TUdf::ArgLhs::from_enc_scalar(lhs);
    let rhs = TUdf::ArgRhs::from_enc_scalar(rhs);

    let result = match (lhs, rhs) {
        (Ok(lhs), Ok(rhs)) => match udf.evaluate(lhs, rhs) {
            Ok(result) => result.into_scalar_value(),
            Err(_) => Ok(encode_scalar_null()),
        },
        _ => match udf.evaluate_error() {
            Ok(result) => result.into_scalar_value(),
            Err(_) => Ok(encode_scalar_null()),
        },
    };
    Ok(ColumnarValue::Scalar(result?))
}
