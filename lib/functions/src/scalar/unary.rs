use crate::{DFResult, FunctionName};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rdf_fusion_encoding::{EncodingArray, EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use rdf_fusion_functions_scalar::{SparqlOpVolatility, UnarySparqlOp};
use rdf_fusion_model::ThinError;
use std::any::Any;

#[macro_export]
macro_rules! impl_unary_sparql_op {
    ($ENCODING: ty, $DECODER: ty, $ENCODER: ty, $FUNCTION_NAME: ident, $SPARQL_OP: ty, $NAME: expr) => {
        pub fn $FUNCTION_NAME() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
            let op = <$SPARQL_OP>::new();
            let udf_impl = crate::scalar::unary::UnaryScalarUdfOp::<
                $SPARQL_OP,
                $ENCODING,
                $DECODER,
                $ENCODER,
            >::new($NAME, op);
            std::sync::Arc::new(
                datafusion::logical_expr::ScalarUDF::new_from_impl(udf_impl),
            )
        }
    };
}

#[derive(Debug)]
pub(crate) struct UnaryScalarUdfOp<TOp, TEncoding, TDecoder, TEncoder>
where
    TOp: UnarySparqlOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    name: String,
    op: TOp,
    signature: Signature,
    _encoding: std::marker::PhantomData<TEncoding>,
    _decoder: std::marker::PhantomData<TDecoder>,
    _encoder: std::marker::PhantomData<TEncoder>,
}

impl<TOp, TEncoding, TDecoder, TEncoder> UnaryScalarUdfOp<TOp, TEncoding, TDecoder, TEncoder>
where
    TOp: UnarySparqlOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
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
            _decoder: Default::default(),
            _encoder: Default::default(),
        }
    }
}

impl<TOp, TEncoding, TDecoder, TEncoder> ScalarUDFImpl
    for UnaryScalarUdfOp<TOp, TEncoding, TDecoder, TEncoder>
where
    TOp: UnarySparqlOp + 'static,
    TEncoding: TermEncoding + 'static,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg<'a>> + 'static,
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
        match TryInto::<[_; 1]>::try_into(args.args) {
            Ok([ColumnarValue::Array(arg)]) => {
                dispatch_unary_array::<TEncoding, TDecoder, TEncoder, TOp>(
                    &self.op,
                    &TEncoding::try_new_array(arg)?,
                )
            }
            Ok([ColumnarValue::Scalar(arg)]) => {
                dispatch_unary_scalar::<TEncoding, TDecoder, TEncoder, TOp>(
                    &self.op,
                    &TEncoding::try_new_scalar(arg)?,
                )
            }
            _ => exec_err!("Unexpected input combination."),
        }
    }
}

fn dispatch_unary_array<'data, TEncoding, TDecoder, TEncoder, TOp>(
    op: &TOp,
    values: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: UnarySparqlOp,
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

fn dispatch_unary_scalar<'data, TEncoding, TDecoder, TEncoder, TOp>(
    op: &TOp,
    value: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: UnarySparqlOp,
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
