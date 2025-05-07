use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::decoders::{
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::OwnedStringLiteralTermValueEncoder;
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::{QuaternaryRdfTermValueOp, SparqlOp};
use graphfusion_functions_scalar::{ReplaceSparqlOp, TernaryRdfTermValueOp};
use itertools::izip;

macro_rules! impl_quarternary_rdf_value_op {
    ($ENCODING: ty, $DECODER0: ty, $DECODER1: ty, $DECODER2: ty, $DECODER3: ty, $ENCODER:ty, $STRUCT_NAME: ident, $SPARQL_OP: ty) => {
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
                dispatch_quaternary::<
                    $ENCODING,
                    $DECODER0,
                    $DECODER1,
                    $DECODER2,
                    $DECODER3,
                    $ENCODER,
                    $SPARQL_OP,
                >(&self.op, &args.args, args.number_rows)
            }
        }
    };
}

// Strings
impl_quarternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    RegexTermValueQuarternaryDispatcher,
    ReplaceSparqlOp
);

pub(crate) fn dispatch_quaternary<TEncoding, TDecoder0, TDecoder1, TDecoder2, TDecoder3, TEncoder, TOp>(
    op: &TOp,
    args: &[ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: QuaternaryRdfTermValueOp,
    TEncoding: TermEncoding,
    TDecoder0: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg0<'a>>,
    TDecoder1: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg1<'a>>,
    TDecoder2: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg2<'a>>,
    TDecoder3: for<'a> TermDecoder<TEncoding, Term<'a> = TOp::Arg3<'a>>,
    TEncoder: for<'a> TermEncoder<TEncoding, Term<'a> = TOp::Result<'a>>,
{
    if args.len() != 3 {
        return exec_err!("Unexpected number of arguments.");
    }

    let arg0 = TEncoding::try_new_datum(args[0].clone(), number_of_rows)?;
    let arg1 = TEncoding::try_new_datum(args[1].clone(), number_of_rows)?;
    let arg2 = TEncoding::try_new_datum(args[2].clone(), number_of_rows)?;
    let arg3 = TEncoding::try_new_datum(args[2].clone(), number_of_rows)?;

    let arg0 = arg0.term_iter::<TDecoder0>();
    let arg1 = arg1.term_iter::<TDecoder1>();
    let arg2 = arg2.term_iter::<TDecoder2>();
    let arg3 = arg3.term_iter::<TDecoder3>();

    let results = izip!(arg0, arg1, arg2, arg3)
        .map(|(arg0, arg1, arg2, arg3)| match (arg0, arg1, arg2, arg3) {
            (Ok(arg0), Ok(arg1), Ok(arg2), Ok(arg3)) => op.evaluate(arg0, arg1, arg2, arg3),
            _ => op.evaluate_error(arg0, arg1, arg2, arg3),
        })
        .collect::<Vec<_>>();
    let result = TEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}
