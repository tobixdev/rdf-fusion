use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::decoders::{
    BooleanTermValueDecoder, DefaultTermValueDecoder, IntegerTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    BooleanTermValueEncoder, DefaultTermValueEncoder, OwnedStringLiteralTermValueEncoder,
    StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::SparqlOp;
use graphfusion_functions_scalar::{
    IfSparqlOp, RegexSparqlOp, ReplaceSparqlOp, SubStrSparqlOp, TernaryRdfTermValueOp,
};
use itertools::izip;

macro_rules! impl_ternary_rdf_value_op {
    ($ENCODING: ty, $DECODER0: ty, $DECODER1: ty, $DECODER2: ty, $ENCODER: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty) => {
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
                dispatch_ternary::<$ENCODING, $DECODER0, $DECODER1, $DECODER2, $ENCODER, $SPARQL_OP>(&self.op, &args.args, args.number_rows)
            }
        }
    };
}

// Functional Forms
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    BooleanTermValueDecoder,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    DefaultTermValueEncoder,
    IfValueTernaryDispatcher,
    IfSparqlOp
);

// Strings
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    RegexValueTernaryDispatcher,
    RegexSparqlOp
);
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    ReplaceValueTernaryDispatcher,
    ReplaceSparqlOp
);
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    SubStrTernaryDispatcher,
    SubStrSparqlOp
);

pub(crate) fn dispatch_ternary<TEncoding, TDecoder0, TDecoder1, TDecoder2, TEncoder, TOp>(
    op: &TOp,
    args: &[ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: TernaryRdfTermValueOp,
    TEncoding: TermEncoding,
    TDecoder0: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg0<'a>>,
    TDecoder1: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg1<'a>>,
    TDecoder2: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg2<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    if args.len() != 3 {
        return exec_err!("Unexpected number of arguments.");
    }

    let arg0 = TEncoding::try_new_datum(args[0].clone(), number_of_rows)?;
    let arg1 = TEncoding::try_new_datum(args[1].clone(), number_of_rows)?;
    let arg2 = TEncoding::try_new_datum(args[2].clone(), number_of_rows)?;

    let arg0 = arg0.term_iter::<TDecoder0>();
    let arg1 = arg1.term_iter::<TDecoder1>();
    let arg2 = arg2.term_iter::<TDecoder2>();

    let results = izip!(arg0, arg1, arg2)
        .map(|(arg0, arg1, arg2)| match (arg0, arg1, arg2) {
            (Ok(arg0), Ok(arg1), Ok(arg2)) => op.evaluate(arg0, arg1, arg2),
            _ => op.evaluate_error(arg0, arg1, arg2),
        })
        .collect::<Vec<_>>();
    let result = TEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}
