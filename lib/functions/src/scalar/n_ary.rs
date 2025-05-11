use crate::{DFResult, FunctionName};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use graphfusion_encoding::{EncodingArray, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::NArySparqlOp;
use graphfusion_functions_scalar::SparqlOpVolatility;
use std::any::Any;

#[macro_export]
macro_rules! impl_n_ary_sparql_op {
    ($ENCODING: ty, $DECODER: ty, $ENCODER: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty, $NAME: expr) => {
        #[derive(Debug)]
        struct $STRUCT_NAME {}

        impl crate::builtin::GraphFusionUdfFactory for $STRUCT_NAME {
            fn name(&self) -> crate::FunctionName {
                crate::FunctionName::Builtin($NAME)
            }

            fn encoding(&self) -> std::vec::Vec<graphfusion_encoding::EncodingName> {
                vec![<$ENCODING as graphfusion_encoding::TermEncoding>::name()]
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
                let udf_impl = crate::scalar::n_ary::NAryScalarUdfOp::<
                    $SPARQL_OP,
                    $ENCODING,
                    $DECODER,
                    $ENCODER,
                >::new(self.name(), op);
                let udf = datafusion::logical_expr::ScalarUDF::new_from_impl(udf_impl);
                Ok(std::sync::Arc::new(udf))
            }
        }
    };
}

#[derive(Debug)]
pub(crate) struct NAryScalarUdfOp<TOp, TEncoding, TDecoder, TEncoder>
where
    TOp: NArySparqlOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Args<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    name: String,
    op: TOp,
    signature: Signature,
    _encoding: std::marker::PhantomData<TEncoding>,
    _decoder: std::marker::PhantomData<TDecoder>,
    _encoder: std::marker::PhantomData<TEncoder>,
}

impl<TOp, TEncoding, TDecoder, TEncoder> NAryScalarUdfOp<TOp, TEncoding, TDecoder, TEncoder>
where
    TOp: NArySparqlOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Args<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    pub(crate) fn new(name: FunctionName, op: TOp) -> Self {
        let volatility = match op.volatility() {
            SparqlOpVolatility::Immutable => Volatility::Immutable,
            SparqlOpVolatility::Stable => Volatility::Stable,
            SparqlOpVolatility::Volatile => Volatility::Volatile,
        };
        let signature = Signature::new(
            TypeSignature::OneOf(vec![
                TypeSignature::Nullary,
                TypeSignature::Variadic(vec![TEncoding::data_type()]),
            ]),
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
    for NAryScalarUdfOp<TOp, TEncoding, TDecoder, TEncoder>
where
    TOp: NArySparqlOp + 'static,
    TEncoding: TermEncoding + 'static,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Args<'a>> + 'static,
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        dispatch_n_ary::<TOp, TEncoding, TDecoder, TEncoder>(
            &self.op,
            args.args.as_slice(),
            args.number_rows,
        )
    }
}

fn dispatch_n_ary<TOp, TEncoding, TDecoder, TEncoder>(
    op: &TOp,
    args: &[ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: NArySparqlOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Args<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    let args = args
        .iter()
        .map(|a| TEncoding::try_new_datum(a.clone(), number_of_rows))
        .collect::<DFResult<Vec<_>>>()?;
    let args_refs = args.iter().collect::<Vec<_>>();

    let mut iters = Vec::new();
    for arg in args_refs {
        iters.push(arg.term_iter::<TDecoder>());
    }

    let results = multi_zip(iters).map(|args| {
        if args.iter().all(Result::is_ok) {
            let args = args.into_iter().map(|arg| arg.unwrap()).collect::<Vec<_>>();
            op.evaluate(args.as_slice())
        } else {
            op.evaluate_error(args.as_slice())
        }
    });
    let result = TEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn multi_zip<I, T>(mut iterators: Vec<I>) -> impl Iterator<Item = Vec<T>>
where
    I: Iterator<Item = T>,
{
    std::iter::from_fn(move || {
        let mut items = Vec::with_capacity(iterators.len());
        for iter in &mut iterators {
            match iter.next() {
                Some(item) => items.push(item),
                None => return None, // Stop if any iterator is exhausted
            }
        }

        if items.is_empty() {
            None
        } else {
            Some(items)
        }
    })
}
