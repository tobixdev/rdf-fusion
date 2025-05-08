use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::decoders::{
    DefaultTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    DefaultTermValueEncoder, OwnedStringLiteralTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, EncodingDatum, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::SparqlOp;
use graphfusion_functions_scalar::TernaryRdfTermValueOp;
use graphfusion_functions_scalar::{CoalesceSparqlOp, ConcatSparqlOp, NAryRdfTermValueOp};

macro_rules! impl_n_ary_rdf_value_op {
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
                dispatch_n_ary::<$ENCODING, $DECODER, $ENCODER, $SPARQL_OP>(
                    &self.op,
                    args.args.as_slice(),
                    args.number_rows,
                )
            }
        }
    };
}

// Functional Forms
impl_n_ary_rdf_value_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DefaultTermValueEncoder,
    CoalesceTermValueDispatcher,
    CoalesceSparqlOp
);

// Strings
impl_n_ary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    ConcatValueTermValueDispatcher,
    ConcatSparqlOp
);

fn dispatch_n_ary<'data, TEncoding, TDecoder, TEncoder, TOp>(
    op: &TOp,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: NAryRdfTermValueOp,
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
