use crate::builtin::BuiltinName;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_datafusion_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use graphfusion_encoding::{EncodingArray, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::{SparqlOpVolatility, TernarySparqlOp};
use itertools::izip;
use std::any::Any;

#[macro_export]
macro_rules! impl_ternary_sparql_op {
    ($ENCODING: ty, $DECODER0: ty, $DECODER1: ty, $DECODER2: ty, $ENCODER: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty, $NAME: expr) => {
        #[derive(Debug)]
        struct $STRUCT_NAME {}

        impl crate::builtin::GraphFusionBuiltinFactory for $STRUCT_NAME {
            fn name(&self) -> crate::builtin::BuiltinName {
                $NAME
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
            ) -> crate::DFResult<datafusion::logical_expr::ScalarUDF> {
                let op = <$SPARQL_OP>::new();
                let udf_impl = crate::scalar::ternary::TernaryScalarUdfOp::<
                    $SPARQL_OP,
                    $ENCODING,
                    $DECODER0,
                    $DECODER1,
                    $DECODER2,
                    $ENCODER,
                >::new($NAME, op);
                Ok(datafusion::logical_expr::ScalarUDF::new_from_impl(udf_impl))
            }
        }
    };
}

#[derive(Debug)]
pub(crate) struct TernaryScalarUdfOp<TOp, TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder>
where
    TOp: TernarySparqlOp,
    TEncoding: TermEncoding,
    TDecoder0: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg0<'a>>,
    TDecoder1: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg1<'a>>,
    TDecoder2: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg2<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    name: String,
    op: TOp,
    signature: Signature,
    _encoding: std::marker::PhantomData<TEncoding>,
    _decoder0: std::marker::PhantomData<TDecoder0>,
    _decoder1: std::marker::PhantomData<TDecoder1>,
    _decoder2: std::marker::PhantomData<TDecoder2>,
    _encoder: std::marker::PhantomData<TEncoder>,
}

impl<TOp, TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder>
    TernaryScalarUdfOp<TOp, TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder>
where
    TOp: TernarySparqlOp,
    TEncoding: TermEncoding,
    TDecoder0: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg0<'a>>,
    TDecoder1: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg1<'a>>,
    TDecoder2: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg2<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    pub(crate) fn new(name: BuiltinName, op: TOp) -> Self {
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
            _decoder0: Default::default(),
            _decoder1: Default::default(),
            _decoder2: Default::default(),
            _encoder: Default::default(),
        }
    }
}

impl<TOp, TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder> ScalarUDFImpl
    for TernaryScalarUdfOp<TOp, TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder>
where
    TOp: TernarySparqlOp + 'static,
    TEncoding: TermEncoding + 'static,
    TDecoder0: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg0<'a>> + 'static,
    TDecoder1: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg1<'a>> + 'static,
    TDecoder2: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg2<'a>> + 'static,
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
        dispatch_ternary::<TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder, TOp>(
            &self.op,
            args.args
                .try_into()
                .map_err(|_| exec_datafusion_err!("Unexpected arguments"))?,
            args.number_rows,
        )
    }
}

pub(crate) fn dispatch_ternary<TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder, TOp>(
    op: &TOp,
    args: [ColumnarValue; 3],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: TernarySparqlOp,
    TEncoding: TermEncoding,
    TDecoder0: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg0<'a>>,
    TDecoder1: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg1<'a>>,
    TDecoder2: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg2<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let arg0 = TEncoding::try_new_datum(args[0].clone(), number_of_rows)?;
    let arg1 = TEncoding::try_new_datum(args[1].clone(), number_of_rows)?;
    let arg2 = TEncoding::try_new_datum(args[2].clone(), number_of_rows)?;

    let arg0 = arg0.term_iter::<TDecoder0>();
    let arg1 = arg1.term_iter::<TDecoder1>();
    let arg2 = arg2.term_iter::<TDecoder2>();

    let results = izip!(arg0, arg1, arg2)
        .map(|(arg0, arg1, arg2)| match (arg0, arg1, arg2) {
            (Ok(arg0), Ok(arg1), Ok(arg2)) => op.evaluate(arg0, arg1, arg2),
            (arg0, arg1, arg2) => op.evaluate_error(arg0, arg1, arg2),
        })
        .collect::<Vec<_>>();
    let result = TEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}
