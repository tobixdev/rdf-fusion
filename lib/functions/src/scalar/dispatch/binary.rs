use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::typed_value::{TypedValueArray, TypedValueEncoding, TypedValueScalar};
use rdf_fusion_encoding::TermEncoder;
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermDecoder};
use rdf_fusion_model::{ThinError, ThinResult, TypedValueRef};

pub fn dispatch_binary_typed_value<'data>(
    lhs: &'data EncodingDatum<TypedValueEncoding>,
    rhs: &'data EncodingDatum<TypedValueEncoding>,
    op: impl for<'a> Fn(TypedValueRef<'a>, TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
    ) -> ThinResult<TypedValueRef<'a>>,
) -> DFResult<ColumnarValue> {
    match (lhs, rhs) {
        (EncodingDatum::Array(lhs), EncodingDatum::Array(rhs)) => {
            dispatch_binary_array_array(&lhs, &rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Array(rhs)) => {
            dispatch_binary_scalar_array(&lhs, &rhs, op, error_op)
        }
        (EncodingDatum::Array(lhs), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_array_scalar(&lhs, &rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_scalar_scalar(&lhs, &rhs, op, error_op)
        }
    }
}

fn dispatch_binary_array_array<'data>(
    lhs: &'data TypedValueArray,
    rhs: &'data TypedValueArray,
    op: impl for<'a> Fn(TypedValueRef<'a>, TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
    ) -> ThinResult<TypedValueRef<'a>>,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultTypedValueDecoder::decode_terms(lhs);
    let rhs = DefaultTypedValueDecoder::decode_terms(rhs);

    let results = lhs
        .zip(rhs)
        .map(|(lhs_value, rhs_value)| apply_binary_op(lhs_value, rhs_value, &op, &error_op));
    let result = DefaultTypedValueEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_binary_scalar_array<'data>(
    lhs: &'data TypedValueScalar,
    rhs: &'data TypedValueArray,
    op: impl for<'a> Fn(TypedValueRef<'a>, TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
    ) -> ThinResult<TypedValueRef<'a>>,
) -> DFResult<ColumnarValue> {
    let results = DefaultTypedValueDecoder::decode_terms(rhs).map(|rhs_value| {
        let lhs_value = DefaultTypedValueDecoder::decode_term(lhs);
        apply_binary_op(lhs_value, rhs_value, &op, &error_op)
    });
    let result = DefaultTypedValueEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_binary_array_scalar<'data>(
    lhs: &'data TypedValueArray,
    rhs: &'data TypedValueScalar,
    op: impl for<'a> Fn(TypedValueRef<'a>, TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
    ) -> ThinResult<TypedValueRef<'a>>,
) -> DFResult<ColumnarValue> {
    let results = DefaultTypedValueDecoder::decode_terms(lhs).map(|lhs_value| {
        let rhs_value = DefaultTypedValueDecoder::decode_term(rhs);
        apply_binary_op(lhs_value, rhs_value, &op, &error_op)
    });
    let result = DefaultTypedValueEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_binary_scalar_scalar<'data>(
    lhs: &'data TypedValueScalar,
    rhs: &'data TypedValueScalar,
    op: impl for<'a> Fn(TypedValueRef<'a>, TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
    ) -> ThinResult<TypedValueRef<'a>>,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultTypedValueDecoder::decode_term(lhs);
    let rhs = DefaultTypedValueDecoder::decode_term(rhs);

    let result = apply_binary_op(lhs, rhs, &op, &error_op);
    Ok(ColumnarValue::Scalar(
        DefaultTypedValueEncoder::encode_term(result)?.into_scalar_value(),
    ))
}

fn apply_binary_op<'data>(
    lhs: ThinResult<TypedValueRef<'data>>,
    rhs: ThinResult<TypedValueRef<'data>>,
    op: impl for<'a> Fn(TypedValueRef<'a>, TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
    ) -> ThinResult<TypedValueRef<'a>>,
) -> ThinResult<TypedValueRef<'data>> {
    match (lhs, rhs) {
        (Ok(lhs_value), Ok(rhs_value)) => op(lhs_value, rhs_value),
        (Err(ThinError::InternalError(internal_err)), _)
        | (_, Err(ThinError::InternalError(internal_err))) => {
            ThinError::internal_error(internal_err)
        }
        (lhs, rhs) => error_op(lhs, rhs),
    }
}
