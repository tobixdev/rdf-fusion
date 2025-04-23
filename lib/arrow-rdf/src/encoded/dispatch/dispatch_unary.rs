use crate::encoded::from_encoded_term::FromEncodedTerm;
use crate::encoded::scalars::encode_scalar_null;
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::EncTermField;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datamodel::{Boolean, RdfOpError, RdfValueRef, TermRef};
use functions_scalar::ScalarUnaryRdfOp;

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
    let value = TUdf::Arg::from_enc_scalar(&value);
    let result = match value {
        Ok(value) => udf.evaluate(value),
        Err(_) => udf.evaluate_error(),
    };
    let result = result
        .and_then(|value| value.into_scalar_value().map_err(|_| RdfOpError))
        .unwrap_or(encode_scalar_null());
    Ok(ColumnarValue::Scalar(result))
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
    let values = as_enc_term_array(values).expect("RDF term array expected");

    let booleans = values.child(EncTermField::Boolean.type_id()).as_boolean();
    if values.len() == booleans.len() {
        let offsets = values.offsets().expect("Always dense");
        return dispatch_unary_array_boolean(udf, offsets, booleans);
    }

    let results = (0..values.len())
        .into_iter()
        .map(|i| TermRef::from_enc_array(values, i).and_then(|term| TUdf::Arg::from_term(term)))
        .map(|v| match v {
            Ok(value) => udf.evaluate(value),
            Err(_) => udf.evaluate_error(),
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
    let results = offsets
        .iter()
        .map(|o| values.value(*o as usize))
        .map(|v| TUdf::Arg::from_term(TermRef::BooleanLiteral(Boolean::from(v))))
        .map(|v| udf.evaluate(v?));
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
