use crate::DFResult;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use graphfusion_encoding::{EncodingArray, EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::UnaryTermValueOp;
use graphfusion_model::ThinError;

#[macro_export]
macro_rules! impl_unary_sparql_op {
    ($ENCODING: ty, $DECODER: ty, $ENCODER: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty) => {
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
                match TryInto::<[_; 1]>::try_into(args.args) {
                    Ok([ColumnarValue::Array(arg)]) => {
                        dispatch_unary_array::<$ENCODING, $DECODER, $ENCODER, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_array(arg)?,
                        )
                    }
                    Ok([ColumnarValue::Scalar(arg)]) => {
                        dispatch_unary_scalar::<$ENCODING, $DECODER, $ENCODER, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_scalar(arg)?,
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

pub(crate) fn dispatch_unary_array<'data, TEncoding, TDecoder, TEncoder, TOp>(
    op: &TOp,
    values: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let results = <TDecoder>::decode_terms(values).map(|v| match v {
        Ok(value) => op.evaluate(value),
        Err(ThinError::Expected) => op.evaluate_error(),
        Err(internal_err) => Err(internal_err),
    });
    let result = <TEncoder>::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub(crate) fn dispatch_unary_scalar<'data, TEncoding, TDecoder, TEncoder, TOp>(
    op: &TOp,
    value: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let value = TDecoder::decode_term(value);
    let result = match value {
        Ok(value) => op.evaluate(value),
        Err(ThinError::Expected) => op.evaluate_error(),
        Err(ThinError::InternalError(error)) => {
            return exec_err!("InternalError in UDF: {}", error)
        }
    };
    let result = TEncoder::encode_term(result)?;
    Ok(ColumnarValue::Scalar(result.into_scalar_value()))
}
