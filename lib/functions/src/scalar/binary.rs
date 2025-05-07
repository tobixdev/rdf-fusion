use crate::DFResult;
use datafusion::logical_expr::ColumnarValue;
use graphfusion_encoding::EncodingArray;
use graphfusion_encoding::{EncodingScalar, TermDecoder};
use graphfusion_encoding::{TermEncoder, TermEncoding};
use graphfusion_functions_scalar::BinaryTermValueOp;
use graphfusion_model::{ThinError, ThinResult};

macro_rules! impl_binary_sparql_op {
    ($ENCODING: ty, $LHS_DECODER: ty, $RHS_DECODER: ty, $ENCODER: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty) => {
        #[derive(Debug)]
        struct $STRUCT_NAME {
            signature: Signature,
            op: $SPARQL_OP,
        }

        impl SparqlOpDispatcher for $STRUCT_NAME {
            fn name(&self) -> &str {
                self.op.name()
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok(<$ENCODING>::data_type())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                match args.args.as_slice() {
                    [ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)] => {
                        crate::scalar::binary::dispatch_binary_array_array::<
                            $ENCODING,
                            $LHS_DECODER,
                            $RHS_DECODER,
                            $ENCODER,
                            $SPARQL_OP,
                        >(
                            &self.op,
                            &<$ENCODING>::try_new_array(lhs.clone())?,
                            &<$ENCODING>::try_new_array(rhs.clone())?,
                        )
                    }
                    [ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)] => {
                        crate::scalar::binary::dispatch_binary_scalar_array::<
                            $ENCODING,
                            $LHS_DECODER,
                            $RHS_DECODER,
                            $ENCODER,
                            $SPARQL_OP,
                        >(
                            &self.op,
                            &<$ENCODING>::try_new_scalar(lhs.clone())?,
                            &<$ENCODING>::try_new_array(rhs.clone())?,
                        )
                    }
                    [ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)] => {
                        crate::scalar::binary::dispatch_binary_array_scalar::<
                            $ENCODING,
                            $LHS_DECODER,
                            $RHS_DECODER,
                            $ENCODER,
                            $SPARQL_OP,
                        >(
                            &self.op,
                            &<$ENCODING>::try_new_array(lhs.clone())?,
                            &<$ENCODING>::try_new_scalar(rhs.clone())?,
                        )
                    }
                    [ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)] => {
                        crate::scalar::binary::dispatch_binary_scalar_scalar::<
                            $ENCODING,
                            $LHS_DECODER,
                            $RHS_DECODER,
                            $ENCODER,
                            $SPARQL_OP,
                        >(
                            &self.op,
                            &<$ENCODING>::try_new_scalar(lhs.clone())?,
                            &<$ENCODING>::try_new_scalar(rhs.clone())?,
                        )
                    }
                    _ => Err(DataFusionError::Execution(String::from(
                        "Unexpected type combination.",
                    ))),
                }
            }
        }
    };
}

pub(crate) fn dispatch_binary_array_array<
    'data,
    TEncoding,
    TLhsDecoder,
    TRhsDecoder,
    TEncoder,
    TOp,
>(
    op: &TOp,
    lhs: &'data TEncoding::Array,
    rhs: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TLhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>>,
    TRhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let lhs = TLhsDecoder::decode_terms(lhs);
    let rhs = TRhsDecoder::decode_terms(rhs);

    let results = lhs
        .zip(rhs)
        .map(|(lhs_value, rhs_value)| apply_binary_op(op, lhs_value, rhs_value));
    let result = TEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub(crate) fn dispatch_binary_scalar_array<
    'data,
    TEncoding,
    TLhsDecoder,
    TRhsDecoder,
    TEncoder,
    TOp,
>(
    op: &TOp,
    lhs: &'data TEncoding::Scalar,
    rhs: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TLhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>>,
    TRhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let lhs_value = TLhsDecoder::decode_term(lhs);

    let results =
        TRhsDecoder::decode_terms(rhs).map(|rhs_value| apply_binary_op(op, lhs_value, rhs_value));
    let result = TEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub(crate) fn dispatch_binary_array_scalar<
    'data,
    TEncoding,
    TLhsDecoder,
    TRhsDecoder,
    TEncoder,
    TOp,
>(
    op: &TOp,
    lhs: &'data TEncoding::Array,
    rhs: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TLhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>>,
    TRhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let rhs_value = TRhsDecoder::decode_term(rhs);

    let results =
        TLhsDecoder::decode_terms(lhs).map(|lhs_value| apply_binary_op(op, lhs_value, rhs_value));
    let result = TEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub(crate) fn dispatch_binary_scalar_scalar<
    'data,
    TEncoding,
    TLhsDecoder,
    TRhsDecoder,
    TEncoder,
    TOp,
>(
    op: &TOp,
    lhs: &'data TEncoding::Scalar,
    rhs: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TLhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>>,
    TRhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let lhs = TLhsDecoder::decode_term(lhs);
    let rhs = TRhsDecoder::decode_term(rhs);

    let result = apply_binary_op(op, lhs, rhs);
    Ok(ColumnarValue::Scalar(
        TEncoder::encode_term(result)?.into_scalar_value(),
    ))
}

fn apply_binary_op<'a, TOp: BinaryTermValueOp>(
    op: &TOp,
    lhs: ThinResult<TOp::ArgLhs<'a>>,
    rhs: ThinResult<TOp::ArgRhs<'a>>,
) -> ThinResult<TOp::Result<'a>> {
    match (lhs, rhs) {
        (Ok(lhs_value), Ok(rhs_value)) => op.evaluate(lhs_value, rhs_value),
        (Err(ThinError::InternalError(internal_err)), _)
        | (_, Err(ThinError::InternalError(internal_err))) => {
            ThinError::internal_error(internal_err)
        }
        _ => op.evaluate_error(lhs, rhs),
    }
}
