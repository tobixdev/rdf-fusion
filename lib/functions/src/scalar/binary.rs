use crate::builtin::BuiltinName;
use crate::{DFResult, FunctionName};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use graphfusion_encoding::EncodingArray;
use graphfusion_encoding::{EncodingScalar, TermDecoder};
use graphfusion_encoding::{TermEncoder, TermEncoding};
use graphfusion_functions_scalar::{BinarySparqlOp, SparqlOpVolatility};
use graphfusion_model::{ThinError, ThinResult};
use std::any::Any;

#[macro_export]
macro_rules! impl_binary_sparql_op {
    ($ENCODING: ty, $DECODER_LHS: ty, $DECODER_RHS: ty, $ENCODER: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty, $NAME: expr) => {
        #[derive(Debug)]
        struct $STRUCT_NAME {}

        impl crate::builtin::GraphFusionUdfFactory for $STRUCT_NAME {
            fn name(&self) -> crate::FunctionName {
                crate::FunctionName::Builtin($NAME)
            }

            fn encoding(&self) -> std::vec::Vec<graphfusion_encoding::EncodingName> {
                vec![<$ENCODING>::name()]
            }

            /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
            fn create_with_args(
                &self,
                _constant_args: std::collections::HashMap<
                    std::string::String,
                    graphfusion_model::Term,
                >,
            ) -> crate::DFResult<std::sync::Arc<datafusion::logical_expr::ScalarUDF>> {
                let op = <$SPARQL_OP>::new();
                let udf_impl = crate::scalar::binary::BinaryScalarUdfOp::<
                    $SPARQL_OP,
                    $ENCODING,
                    $DECODER_LHS,
                    $DECODER_RHS,
                    $ENCODER,
                >::new(self.name(), op);
                let udf = datafusion::logical_expr::ScalarUDF::new_from_impl(udf_impl);
                Ok(std::sync::Arc::new(udf))
            }
        }
    };
}

#[derive(Debug)]
pub(crate) struct BinaryScalarUdfOp<TOp, TEncoding, TDecoderLhs, TDecoderRhs, TEncoder>
where
    TOp: BinarySparqlOp,
    TEncoding: TermEncoding,
    TDecoderLhs: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>>,
    TDecoderRhs: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    name: String,
    op: TOp,
    signature: Signature,
    _encoding: std::marker::PhantomData<TEncoding>,
    _decoder_lhs: std::marker::PhantomData<TDecoderLhs>,
    _decoder_rhs: std::marker::PhantomData<TDecoderRhs>,
    _encoder: std::marker::PhantomData<TEncoder>,
}

impl<TOp, TEncoding, TDecoderLhs, TDecoderRhs, TEncoder>
    BinaryScalarUdfOp<TOp, TEncoding, TDecoderLhs, TDecoderRhs, TEncoder>
where
    TOp: BinarySparqlOp + 'static,
    TEncoding: TermEncoding + 'static,
    TDecoderLhs: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>> + 'static,
    TDecoderRhs: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>> + 'static,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>> + 'static,
{
    pub(crate) fn new(name: FunctionName, op: TOp) -> Self {
        let volatility = match op.volatility() {
            SparqlOpVolatility::Immutable => Volatility::Immutable,
            SparqlOpVolatility::Stable => Volatility::Stable,
            SparqlOpVolatility::Volatile => Volatility::Volatile,
        };
        let signature = Signature::new(
            TypeSignature::Uniform(1, vec![TEncoding::data_type()]),
            volatility,
        );
        Self {
            name: name.to_string(),
            op,
            signature,
            _encoding: Default::default(),
            _decoder_lhs: Default::default(),
            _decoder_rhs: Default::default(),
            _encoder: Default::default(),
        }
    }
}

impl<TOp, TEncoding, TDecoderLhs, TDecoderRhs, TEncoder> ScalarUDFImpl
    for BinaryScalarUdfOp<TOp, TEncoding, TDecoderLhs, TDecoderRhs, TEncoder>
where
    TOp: BinarySparqlOp + 'static,
    TEncoding: TermEncoding + 'static,
    TDecoderLhs: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>> + 'static,
    TDecoderRhs: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>> + 'static,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>> + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(TEncoding::data_type())
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs<'_>,
    ) -> datafusion::common::Result<ColumnarValue> {
        match TryInto::<[_; 2]>::try_into(args.args) {
            Ok([ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)]) => {
                dispatch_binary_array_array::<TEncoding, TDecoderLhs, TDecoderRhs, TEncoder, TOp>(
                    &self.op,
                    &TEncoding::try_new_array(lhs)?,
                    &TEncoding::try_new_array(rhs)?,
                )
            }
            Ok([ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)]) => {
                dispatch_binary_scalar_array::<TEncoding, TDecoderLhs, TDecoderRhs, TEncoder, TOp>(
                    &self.op,
                    &TEncoding::try_new_scalar(lhs.clone())?,
                    &TEncoding::try_new_array(rhs.clone())?,
                )
            }
            Ok([ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)]) => {
                dispatch_binary_array_scalar::<TEncoding, TDecoderLhs, TDecoderRhs, TEncoder, TOp>(
                    &self.op,
                    &TEncoding::try_new_array(lhs.clone())?,
                    &TEncoding::try_new_scalar(rhs.clone())?,
                )
            }
            Ok([ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)]) => {
                dispatch_binary_scalar_scalar::<TEncoding, TDecoderLhs, TDecoderRhs, TEncoder, TOp>(
                    &self.op,
                    &TEncoding::try_new_scalar(lhs)?,
                    &TEncoding::try_new_scalar(rhs.clone())?,
                )
            }
            _ => exec_err!("Unexpected type combination."),
        }
    }
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
    TOp: BinarySparqlOp,
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
    TOp: BinarySparqlOp,
    TEncoding: TermEncoding,
    TLhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>>,
    TRhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let results = TRhsDecoder::decode_terms(rhs).map(|rhs_value| {
        let lhs_value = TLhsDecoder::decode_term(lhs);
        apply_binary_op(op, lhs_value, rhs_value)
    });
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
    TOp: BinarySparqlOp,
    TEncoding: TermEncoding,
    TLhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgLhs<'a>>,
    TRhsDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::ArgRhs<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let results = TLhsDecoder::decode_terms(lhs).map(|lhs_value| {
        let rhs_value = TRhsDecoder::decode_term(rhs);
        apply_binary_op(op, lhs_value, rhs_value)
    });
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
    TOp: BinarySparqlOp,
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

fn apply_binary_op<'a, TOp: BinarySparqlOp>(
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
        (lhs, rhs) => op.evaluate_error(lhs, rhs),
    }
}
