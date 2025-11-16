use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::TermEncoder;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermEncoding, PlainTermScalar,
};
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::{
    TypedValueArray, TypedValueEncoding, TypedValueEncodingRef, TypedValueScalar,
};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermDecoder};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{TermRef, ThinResult, TypedValue, TypedValueRef};

pub fn dispatch_binary_typed_value<'data>(
    encoding: &TypedValueEncodingRef,
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
            dispatch_binary_typed_value_array_array(encoding, lhs, rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Array(rhs)) => {
            dispatch_binary_typed_value_scalar_array(encoding, lhs, rhs, op, error_op)
        }
        (EncodingDatum::Array(lhs), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_typed_value_array_scalar(encoding, lhs, rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_typed_value_scalar_scalar(encoding, lhs, rhs, op, error_op)
        }
    }
}

fn dispatch_binary_typed_value_array_array<'data>(
    encoding: &TypedValueEncodingRef,
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

    let results = lhs.zip(rhs).map(|(lhs_value, rhs_value)| {
        apply_binary_op(lhs_value, rhs_value, &op, &error_op)
    });
    let result = encoding.default_encoder().encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_typed_value_scalar_array<'data>(
    encoding: &TypedValueEncodingRef,
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
    let result = encoding.default_encoder().encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_typed_value_array_scalar<'data>(
    encoding: &TypedValueEncodingRef,
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
    let result = encoding.default_encoder().encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_typed_value_scalar_scalar<'data>(
    encoding: &TypedValueEncodingRef,
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
        encoding
            .default_encoder()
            .encode_term(result)?
            .into_scalar_value(),
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
        (lhs, rhs) => error_op(lhs, rhs),
    }
}

pub fn dispatch_binary_owned_typed_value(
    encoding: &TypedValueEncodingRef,
    lhs: &EncodingDatum<TypedValueEncoding>,
    rhs: &EncodingDatum<TypedValueEncoding>,
    op: impl Fn(TypedValueRef<'_>, TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn(
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
    ) -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    match (lhs, rhs) {
        (EncodingDatum::Array(lhs), EncodingDatum::Array(rhs)) => {
            dispatch_binary_owned_array_array(encoding, lhs, rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Array(rhs)) => {
            dispatch_binary_owned_scalar_array(encoding, lhs, rhs, op, error_op)
        }
        (EncodingDatum::Array(lhs), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_owned_array_scalar(encoding, lhs, rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_owned_scalar_scalar(encoding, lhs, rhs, op, error_op)
        }
    }
}

fn dispatch_binary_owned_array_array(
    encoding: &TypedValueEncodingRef,
    lhs: &TypedValueArray,
    rhs: &TypedValueArray,
    op: impl Fn(TypedValueRef<'_>, TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn(
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
    ) -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultTypedValueDecoder::decode_terms(lhs);
    let rhs = DefaultTypedValueDecoder::decode_terms(rhs);

    let results = lhs
        .zip(rhs)
        .map(|(lhs_value, rhs_value)| {
            apply_binary_owned_op(lhs_value, rhs_value, &op, &error_op)
        })
        .collect::<Vec<_>>();

    let result_refs = results
        .iter()
        .map(|result| match result {
            Ok(value) => Ok(value.as_ref()),
            Err(err) => Err(*err),
        })
        .collect::<Vec<_>>();
    let result = encoding.default_encoder().encode_terms(result_refs)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_owned_scalar_array(
    encoding: &TypedValueEncodingRef,
    lhs: &TypedValueScalar,
    rhs: &TypedValueArray,
    op: impl Fn(TypedValueRef<'_>, TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn(
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
    ) -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    let results = DefaultTypedValueDecoder::decode_terms(rhs)
        .map(|rhs_value| {
            let lhs_value = DefaultTypedValueDecoder::decode_term(lhs);
            apply_binary_owned_op(lhs_value, rhs_value, &op, &error_op)
        })
        .collect::<Vec<_>>();

    let result_refs = results
        .iter()
        .map(|result| match result {
            Ok(value) => Ok(value.as_ref()),
            Err(err) => Err(*err),
        })
        .collect::<Vec<_>>();
    let result = encoding.default_encoder().encode_terms(result_refs)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_owned_array_scalar(
    encoding: &TypedValueEncodingRef,
    lhs: &TypedValueArray,
    rhs: &TypedValueScalar,
    op: impl Fn(TypedValueRef<'_>, TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn(
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
    ) -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    let results = DefaultTypedValueDecoder::decode_terms(lhs)
        .map(|lhs_value| {
            let rhs_value = DefaultTypedValueDecoder::decode_term(rhs);
            apply_binary_owned_op(lhs_value, rhs_value, &op, &error_op)
        })
        .collect::<Vec<_>>();

    let result_refs = results
        .iter()
        .map(|result| match result {
            Ok(value) => Ok(value.as_ref()),
            Err(err) => Err(*err),
        })
        .collect::<Vec<_>>();
    let result = encoding.default_encoder().encode_terms(result_refs)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_owned_scalar_scalar(
    encoding: &TypedValueEncodingRef,
    lhs: &TypedValueScalar,
    rhs: &TypedValueScalar,
    op: impl Fn(TypedValueRef<'_>, TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn(
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
    ) -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultTypedValueDecoder::decode_term(lhs);
    let rhs = DefaultTypedValueDecoder::decode_term(rhs);

    let result = apply_binary_owned_op(lhs, rhs, &op, &error_op);
    let result_ref = match result.as_ref() {
        Ok(typed_value) => Ok(typed_value.as_ref()),
        Err(err) => Err(*err),
    };
    Ok(ColumnarValue::Scalar(
        encoding
            .default_encoder()
            .encode_term(result_ref)?
            .into_scalar_value(),
    ))
}

fn apply_binary_owned_op(
    lhs: ThinResult<TypedValueRef<'_>>,
    rhs: ThinResult<TypedValueRef<'_>>,
    op: impl Fn(TypedValueRef<'_>, TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn(
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
    ) -> ThinResult<TypedValue>,
) -> ThinResult<TypedValue> {
    match (lhs, rhs) {
        (Ok(lhs_value), Ok(rhs_value)) => op(lhs_value, rhs_value),
        (lhs, rhs) => error_op(lhs, rhs),
    }
}

pub fn dispatch_binary_plain_term<'data>(
    lhs: &'data EncodingDatum<PlainTermEncoding>,
    rhs: &'data EncodingDatum<PlainTermEncoding>,
    op: impl for<'a> Fn(TermRef<'a>, TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TermRef<'a>>,
        ThinResult<TermRef<'a>>,
    ) -> ThinResult<TermRef<'a>>,
) -> DFResult<ColumnarValue> {
    match (lhs, rhs) {
        (EncodingDatum::Array(lhs), EncodingDatum::Array(rhs)) => {
            dispatch_binary_plain_term_array_array(lhs, rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Array(rhs)) => {
            dispatch_binary_plain_term_scalar_array(lhs, rhs, op, error_op)
        }
        (EncodingDatum::Array(lhs), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_plain_term_array_scalar(lhs, rhs, op, error_op)
        }
        (EncodingDatum::Scalar(lhs, _), EncodingDatum::Scalar(rhs, _)) => {
            dispatch_binary_plain_term_scalar_scalar(lhs, rhs, op, error_op)
        }
    }
}

fn dispatch_binary_plain_term_array_array<'data>(
    lhs: &'data PlainTermArray,
    rhs: &'data PlainTermArray,
    op: impl for<'a> Fn(TermRef<'a>, TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TermRef<'a>>,
        ThinResult<TermRef<'a>>,
    ) -> ThinResult<TermRef<'a>>,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultPlainTermDecoder::decode_terms(lhs);
    let rhs = DefaultPlainTermDecoder::decode_terms(rhs);

    let results = lhs.zip(rhs).map(|(lhs_value, rhs_value)| {
        apply_binary_op_plain_term(lhs_value, rhs_value, &op, &error_op)
    });
    let result = DefaultPlainTermEncoder::default().encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_plain_term_scalar_array<'data>(
    lhs: &'data PlainTermScalar,
    rhs: &'data PlainTermArray,
    op: impl for<'a> Fn(TermRef<'a>, TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TermRef<'a>>,
        ThinResult<TermRef<'a>>,
    ) -> ThinResult<TermRef<'a>>,
) -> DFResult<ColumnarValue> {
    let results = DefaultPlainTermDecoder::decode_terms(rhs).map(|rhs_value| {
        let lhs_value = DefaultPlainTermDecoder::decode_term(lhs);
        apply_binary_op_plain_term(lhs_value, rhs_value, &op, &error_op)
    });
    let result = DefaultPlainTermEncoder::default().encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_plain_term_array_scalar<'data>(
    lhs: &'data PlainTermArray,
    rhs: &'data PlainTermScalar,
    op: impl for<'a> Fn(TermRef<'a>, TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TermRef<'a>>,
        ThinResult<TermRef<'a>>,
    ) -> ThinResult<TermRef<'a>>,
) -> DFResult<ColumnarValue> {
    let results = DefaultPlainTermDecoder::decode_terms(lhs).map(|lhs_value| {
        let rhs_value = DefaultPlainTermDecoder::decode_term(rhs);
        apply_binary_op_plain_term(lhs_value, rhs_value, &op, &error_op)
    });
    let result = DefaultPlainTermEncoder::default().encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn dispatch_binary_plain_term_scalar_scalar<'data>(
    lhs: &'data PlainTermScalar,
    rhs: &'data PlainTermScalar,
    op: impl for<'a> Fn(TermRef<'a>, TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TermRef<'a>>,
        ThinResult<TermRef<'a>>,
    ) -> ThinResult<TermRef<'a>>,
) -> DFResult<ColumnarValue> {
    let lhs = DefaultPlainTermDecoder::decode_term(lhs);
    let rhs = DefaultPlainTermDecoder::decode_term(rhs);

    let result = apply_binary_op_plain_term(lhs, rhs, &op, &error_op);
    Ok(ColumnarValue::Scalar(
        DefaultPlainTermEncoder::default()
            .encode_term(result)?
            .into_scalar_value(),
    ))
}

fn apply_binary_op_plain_term<'data>(
    lhs: ThinResult<TermRef<'data>>,
    rhs: ThinResult<TermRef<'data>>,
    op: impl for<'a> Fn(TermRef<'a>, TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TermRef<'a>>,
        ThinResult<TermRef<'a>>,
    ) -> ThinResult<TermRef<'a>>,
) -> ThinResult<TermRef<'data>> {
    match (lhs, rhs) {
        (Ok(lhs_value), Ok(rhs_value)) => op(lhs_value, rhs_value),
        (lhs, rhs) => error_op(lhs, rhs),
    }
}
