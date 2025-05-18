use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rdf_fusion_encoding::{EncodingArray, TermEncoder, TermEncoding};
use rdf_fusion_functions_scalar::{NullarySparqlOp, SparqlOpVolatility};
use std::any::Any;
use datafusion::common::exec_err;

#[macro_export]
macro_rules! impl_nullary_op {
    ($ENCODING: ty, $ENCODER: ty, $FUNCTION_NAME:ident, $SPARQL_OP:ty, $NAME: expr) => {
        pub fn $FUNCTION_NAME() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
            let op = <$SPARQL_OP>::new();
            let udf_impl = crate::scalar::nullary::NullaryScalarUdfOp::<
                $SPARQL_OP,
                $ENCODING,
                $ENCODER,
            >::new($NAME, op);
            let udf = datafusion::logical_expr::ScalarUDF::new_from_impl(udf_impl);
            std::sync::Arc::new(udf)
        }
    };
}

#[derive(Debug)]
pub(crate) struct NullaryScalarUdfOp<TOp, TEncoding, TEncoder>
where
    TOp: NullarySparqlOp,
    TEncoding: TermEncoding,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result>,
{
    name: String,
    op: TOp,
    signature: Signature,
    _encoding: std::marker::PhantomData<TEncoding>,
    _encoder: std::marker::PhantomData<TEncoder>,
}

impl<TOp, TEncoding, TEncoder> NullaryScalarUdfOp<TOp, TEncoding, TEncoder>
where
    TOp: NullarySparqlOp,
    TEncoding: TermEncoding,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result>,
{
    pub(crate) fn new(name: FunctionName, op: TOp) -> Self {
        let volatility = match op.volatility() {
            SparqlOpVolatility::Immutable => Volatility::Immutable,
            SparqlOpVolatility::Stable => Volatility::Stable,
            SparqlOpVolatility::Volatile => Volatility::Volatile,
        };
        Self {
            name: name.to_string(),
            op,
            signature: Signature::new(TypeSignature::Nullary, volatility),
            _encoding: Default::default(),
            _encoder: Default::default(),
        }
    }
}

impl<TOp, TEncoding, TEncoder> ScalarUDFImpl for NullaryScalarUdfOp<TOp, TEncoding, TEncoder>
where
    TOp: NullarySparqlOp + 'static,
    TEncoding: TermEncoding + 'static,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result> + 'static,
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
        if args.args.len() != 0 {
            return exec_err!("Nullary function must have no arguments.")
        }

        let results = (0..args.number_rows).map(|_| self.op.evaluate());
        let result = TEncoder::encode_terms(results)?;
        Ok(ColumnarValue::Array(result.into_array()))
    }
}
