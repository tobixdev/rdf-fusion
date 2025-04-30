use crate::value_encoding::from_encoded_term::FromEncodedTerm;
use crate::value_encoding::scalars::encode_scalar_null;
use crate::value_encoding::write_enc_term::WriteEncTerm;
use crate::value_encoding::RdfValueEncodingField;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::common::{exec_datafusion_err, exec_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use functions_scalar::ScalarUnaryRdfOp;
use model::{Boolean, InternalTermRef, RdfValueRef, ThinError};

pub fn dispatch_unary<'data, TUdf>(
    udf: &TUdf,
    args: &'data [ColumnarValue],
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarUnaryRdfOp,
    TUdf::Arg<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    match args {
        [ColumnarValue::Array(lhs)] => dispatch_unary_array(udf, lhs),
        [ColumnarValue::Scalar(lhs)] => dispatch_unary_scalar(udf, lhs),
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn dispatch_unary_scalar<'data, TUdf>(
    udf: &TUdf,
    value: &'data ScalarValue,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarUnaryRdfOp,
    TUdf::Arg<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    let value = TUdf::Arg::from_enc_scalar(value);
    let result = match value {
        Ok(value) => udf.evaluate(value),
        Err(ThinError::Expected) => udf.evaluate_error(),
        Err(ThinError::InternalError(error)) => {
            return exec_err!("InternalError in UDF: {}", error)
        }
    };
    let result = match result {
        Ok(result) => result.into_scalar_value(),
        Err(_) => Ok(encode_scalar_null()),
    };
    Ok(ColumnarValue::Scalar(result?))
}

fn dispatch_unary_array<'data, TUdf>(
    udf: &TUdf,
    values: &'data dyn Array,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarUnaryRdfOp,
    TUdf::Arg<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    let values = as_enc_term_array(values)?;

    let booleans = values
        .child(RdfValueEncodingField::Boolean.type_id())
        .as_boolean();
    if values.len() == booleans.len() {
        let offsets = values.offsets().ok_or(exec_datafusion_err!(
            "RDF term array should always have offsets."
        ))?;
        return dispatch_unary_array_boolean(udf, offsets, booleans);
    }

    let results = (0..values.len())
        .map(|i| InternalTermRef::from_enc_array(values, i).and_then(TUdf::Arg::from_term))
        .map(|v| match v {
            Ok(value) => udf.evaluate(value),
            Err(ThinError::Expected) => udf.evaluate_error(),
            Err(internal_err) => Err(internal_err),
        });
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

#[inline(never)]
fn dispatch_unary_array_boolean<'data, TUdf>(
    udf: &TUdf,
    offsets: &ScalarBuffer<i32>,
    values: &BooleanArray,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarUnaryRdfOp,
    TUdf::Arg<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    #[allow(clippy::cast_sign_loss, reason = "Offset is always positive")]
    let results = offsets
        .iter()
        .map(|o| values.value(*o as usize))
        .map(|v| TUdf::Arg::from_term(InternalTermRef::BooleanLiteral(Boolean::from(v))))
        .map(|v| udf.evaluate(v?));
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
