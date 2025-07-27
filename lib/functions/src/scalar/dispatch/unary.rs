use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermEncoding, PlainTermScalar};
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::typed_value::{TypedValueArray, TypedValueEncoding, TypedValueScalar};
use rdf_fusion_encoding::TermEncoder;
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermDecoder};
use rdf_fusion_model::{TermRef, ThinResult, TypedValue, TypedValueRef};

pub fn dispatch_unary_typed_value<'data>(
    arg0: &'data EncodingDatum<TypedValueEncoding>,
    op: impl for<'a> Fn(TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl Fn() -> ThinResult<TypedValueRef<'static>>,
) -> DFResult<ColumnarValue> {
    match arg0 {
        EncodingDatum::Array(arg0) => dispatch_unary_array(arg0, op, error_op),
        EncodingDatum::Scalar(arg0, _) => dispatch_unary_scalar(arg0, op, error_op),
    }
}

fn dispatch_unary_array<'data>(
    values: &'data TypedValueArray,
    op: impl for<'a> Fn(TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl Fn() -> ThinResult<TypedValueRef<'static>>,
) -> DFResult<ColumnarValue> {
    let results = DefaultTypedValueDecoder::decode_terms(values).map(|v| match v {
        Ok(value) => op(value),
        Err(_) => error_op(),
    });
    let result = DefaultTypedValueEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_unary_scalar<'data>(
    value: &'data TypedValueScalar,
    op: impl for<'a> Fn(TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl Fn() -> ThinResult<TypedValueRef<'static>>,
) -> DFResult<ColumnarValue> {
    let value = DefaultTypedValueDecoder::decode_term(value);
    let result = match value {
        Ok(value) => op(value),
        Err(_) => error_op(),
    };
    let result = DefaultTypedValueEncoder::encode_term(result)?;
    Ok(ColumnarValue::Scalar(result.into_scalar_value()))
}

pub fn dispatch_unary_owned_typed_value(
    arg0: &EncodingDatum<TypedValueEncoding>,
    op: impl Fn(TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn() -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    match arg0 {
        EncodingDatum::Array(arg0) => dispatch_unary_owned_typed_value_array(arg0, op, error_op),
        EncodingDatum::Scalar(arg0, _) => {
            dispatch_unary_owned_typed_value_scalar(arg0, op, error_op)
        }
    }
}

fn dispatch_unary_owned_typed_value_array(
    values: &TypedValueArray,
    op: impl Fn(TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn() -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    let results = DefaultTypedValueDecoder::decode_terms(values)
        .map(|v| match v {
            Ok(value) => op(value),
            Err(_) => error_op(),
        })
        .collect::<Vec<_>>();

    let result_refs = results
        .iter()
        .map(|result| match result {
            Ok(value) => Ok(value.as_ref()),
            Err(err) => Err(*err),
        })
        .collect::<Vec<_>>();
    let result = DefaultTypedValueEncoder::encode_terms(result_refs)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_unary_owned_typed_value_scalar(
    value: &TypedValueScalar,
    op: impl Fn(TypedValueRef<'_>) -> ThinResult<TypedValue>,
    error_op: impl Fn() -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    let value = DefaultTypedValueDecoder::decode_term(value);
    let result = match value {
        Ok(value) => op(value),
        Err(_) => error_op(),
    };

    let result_ref = match result.as_ref() {
        Ok(typed_value) => Ok(typed_value.as_ref()),
        Err(err) => Err(*err),
    };
    let result = DefaultTypedValueEncoder::encode_term(result_ref)?;
    Ok(ColumnarValue::Scalar(result.into_scalar_value()))
}

pub fn dispatch_unary_plain_term<'data>(
    arg0: &'data EncodingDatum<PlainTermEncoding>,
    op: impl for<'a> Fn(TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl Fn() -> ThinResult<TermRef<'static>>,
) -> DFResult<ColumnarValue> {
    match arg0 {
        EncodingDatum::Array(arg0) => dispatch_unary_plain_term_array(arg0, op, error_op),
        EncodingDatum::Scalar(arg0, _) => dispatch_unary_plain_term_scalar(arg0, op, error_op),
    }
}

fn dispatch_unary_plain_term_array<'data>(
    values: &'data PlainTermArray,
    op: impl for<'a> Fn(TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl Fn() -> ThinResult<TermRef<'static>>,
) -> DFResult<ColumnarValue> {
    let results = DefaultPlainTermDecoder::decode_terms(values).map(|v| match v {
        Ok(value) => op(value),
        Err(_) => error_op(),
    });
    let result = DefaultPlainTermEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_unary_plain_term_scalar<'data>(
    value: &'data PlainTermScalar,
    op: impl for<'a> Fn(TermRef<'a>) -> ThinResult<TermRef<'a>>,
    error_op: impl Fn() -> ThinResult<TermRef<'static>>,
) -> DFResult<ColumnarValue> {
    let value = DefaultPlainTermDecoder::decode_term(value);
    let result = match value {
        Ok(value) => op(value),
        Err(_) => error_op(),
    };
    let result = DefaultPlainTermEncoder::encode_term(result)?;
    Ok(ColumnarValue::Scalar(result.into_scalar_value()))
}
