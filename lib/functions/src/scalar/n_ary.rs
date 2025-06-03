use crate::{DFResult, FunctionName};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rdf_fusion_encoding::{EncodingArray, TermDecoder, TermEncoder, TermEncoding};
use rdf_fusion_functions_scalar::NArySparqlOp;
use rdf_fusion_functions_scalar::SparqlOpVolatility;
use std::any::Any;
use std::marker::PhantomData;

#[macro_export]
macro_rules! impl_n_ary_sparql_op {
    ($ENCODING: ty, $DECODER: ty, $ENCODER: ty, $FUNCTION_NAME: ident, $SPARQL_OP: ty, $NAME: expr) => {
        pub fn $FUNCTION_NAME() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
            let op = <$SPARQL_OP>::new();
            let udf_impl = $crate::scalar::n_ary::NAryScalarUdfOp::<
                $SPARQL_OP,
                $ENCODING,
                $DECODER,
                $ENCODER,
            >::new(&$NAME, op);
            let udf = datafusion::logical_expr::ScalarUDF::new_from_impl(udf_impl);
            std::sync::Arc::new(udf)
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
    _encoding: PhantomData<TEncoding>,
    _decoder: PhantomData<TDecoder>,
    _encoder: PhantomData<TEncoder>,
}

impl<TOp, TEncoding, TDecoder, TEncoder> NAryScalarUdfOp<TOp, TEncoding, TDecoder, TEncoder>
where
    TOp: NArySparqlOp,
    TEncoding: TermEncoding,
    TDecoder: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Args<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    pub(crate) fn new(name: &FunctionName, op: TOp) -> Self {
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
            _encoding: PhantomData,
            _decoder: PhantomData,
            _encoder: PhantomData,
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
    if args.is_empty() {
        let results = (0..number_of_rows).map(|_| op.evaluate(&[]));
        let result = TEncoder::encode_terms(results)?;
        return Ok(ColumnarValue::Array(result.into_array()));
    }

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
