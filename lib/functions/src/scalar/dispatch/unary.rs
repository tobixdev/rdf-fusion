use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::typed_value::{TypedValueArray, TypedValueEncoding, TypedValueScalar};
use rdf_fusion_encoding::TermEncoder;
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermDecoder};
use rdf_fusion_model::{ThinError, ThinResult, TypedValueRef};

pub fn dispatch_unary_typed_value<'data>(
    arg0: &'data EncodingDatum<TypedValueEncoding>,
    op: impl for<'a> Fn(TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl Fn() -> ThinResult<TypedValueRef<'static>>,
) -> DFResult<ColumnarValue> {
    match arg0 {
        EncodingDatum::Array(arg0) => dispatch_unary_array(arg0, op, error_op),
        EncodingDatum::Scalar(arg0, _) => dispatch_unary_scalar(&arg0, op, error_op),
    }
}

fn dispatch_unary_array<'data>(
    values: &'data TypedValueArray,
    op: impl for<'a> Fn(TypedValueRef<'a>) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl Fn() -> ThinResult<TypedValueRef<'static>>,
) -> DFResult<ColumnarValue> {
    let results = DefaultTypedValueDecoder::decode_terms(values).map(|v| match v {
        Ok(value) => op(value),
        Err(ThinError::Expected) => error_op(),
        Err(internal_err) => Err(internal_err),
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
        Err(ThinError::Expected) => error_op(),
        Err(ThinError::InternalError(error)) => {
            return exec_err!("InternalError in UDF: {}", error)
        }
    };
    let result = DefaultTypedValueEncoder::encode_term(result)?;
    Ok(ColumnarValue::Scalar(result.into_scalar_value()))
}
