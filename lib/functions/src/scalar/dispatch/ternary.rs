use datafusion::logical_expr::ColumnarValue;
use itertools::izip;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, TermEncoder};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{ThinResult, TypedValue, TypedValueRef};

pub fn dispatch_ternary_typed_value<'data>(
    arg0: &'data EncodingDatum<TypedValueEncoding>,
    arg1: &'data EncodingDatum<TypedValueEncoding>,
    arg2: &'data EncodingDatum<TypedValueEncoding>,
    op: impl for<'a> Fn(
        TypedValueRef<'a>,
        TypedValueRef<'a>,
        TypedValueRef<'a>,
    ) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
        ThinResult<TypedValueRef<'a>>,
    ) -> ThinResult<TypedValueRef<'a>>,
) -> DFResult<ColumnarValue> {
    let arg0 = arg0.term_iter::<DefaultTypedValueDecoder>();
    let arg1 = arg1.term_iter::<DefaultTypedValueDecoder>();
    let arg2 = arg2.term_iter::<DefaultTypedValueDecoder>();

    let results = izip!(arg0, arg1, arg2)
        .map(|(arg0, arg1, arg2)| match (arg0, arg1, arg2) {
            (Ok(arg0), Ok(arg1), Ok(arg2)) => op(arg0, arg1, arg2),
            (arg0, arg1, arg2) => error_op(arg0, arg1, arg2),
        })
        .collect::<Vec<_>>();
    let result = DefaultTypedValueEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

pub fn dispatch_ternary_owned_typed_value<'data>(
    arg0: &'data EncodingDatum<TypedValueEncoding>,
    arg1: &'data EncodingDatum<TypedValueEncoding>,
    arg2: &'data EncodingDatum<TypedValueEncoding>,
    op: impl Fn(
        TypedValueRef<'_>,
        TypedValueRef<'_>,
        TypedValueRef<'_>,
    ) -> ThinResult<TypedValue>,
    error_op: impl Fn(
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
        ThinResult<TypedValueRef<'_>>,
    ) -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    let arg0 = arg0.term_iter::<DefaultTypedValueDecoder>();
    let arg1 = arg1.term_iter::<DefaultTypedValueDecoder>();
    let arg2 = arg2.term_iter::<DefaultTypedValueDecoder>();

    let results = izip!(arg0, arg1, arg2)
        .map(|(arg0, arg1, arg2)| match (arg0, arg1, arg2) {
            (Ok(arg0), Ok(arg1), Ok(arg2)) => op(arg0, arg1, arg2),
            (arg0, arg1, arg2) => error_op(arg0, arg1, arg2),
        })
        .collect::<Vec<_>>();
    let results_iter = results.iter().map(|r| match r {
        Ok(res) => Ok(res.as_ref()),
        Err(err) => Err(*err),
    });
    let result = DefaultTypedValueEncoder::encode_terms(results_iter)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}
