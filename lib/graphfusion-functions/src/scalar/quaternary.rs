use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::ReplaceSparqlOp;
use graphfusion_functions_scalar::{QuaternaryRdfTermValueOp, SparqlOp};

macro_rules! impl_quarternary_rdf_value_op {
    ($ENCODING: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty) => {
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
                dispatch_quaternary::<$ENCODING, $SPARQL_OP>(&self.op, &args.args, args.number_rows)
            }
        }
    };
}

// Strings
impl_quarternary_rdf_value_op!(
    TermValueEncoding,
    RegexTermValueQuarternaryDispatcher,
    ReplaceSparqlOp
);

pub fn dispatch_quaternary<'data, TEncoding, TOp>(
    op: &TOp,
    args: &[ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: QuaternaryRdfTermValueOp,
    TEncoding: TermEncoding,
    TEncoding: TermDecoder<'data, TOp::Arg0<'data>>,
    TEncoding: TermDecoder<'data, TOp::Arg1<'data>>,
    TEncoding: TermDecoder<'data, TOp::Arg2<'data>>,
    TEncoding: TermDecoder<'data, TOp::Arg3<'data>>,
    TEncoding: TermEncoder<TOp::Result<'data>>,
{
    if args.len() != 4 {
        return exec_err!("Unexpected number of arguments.");
    }

    let arg0 = TEncoding::try_new_datum(args[0].clone(), number_of_rows)?;
    let arg1 = TEncoding::try_new_datum(args[1].clone(), number_of_rows)?;
    let arg2 = TEncoding::try_new_datum(args[2].clone(), number_of_rows)?;
    let arg3 = TEncoding::try_new_datum(args[3].clone(), number_of_rows)?;

    let iter0 = arg0.boxed_iter::<TOp::Arg0<'data>>();
    let iter1 = arg1.boxed_iter::<TOp::Arg1<'data>>();
    let iter2 = arg2.boxed_iter::<TOp::Arg2<'data>>();
    let iter3 = arg3.boxed_iter::<TOp::Arg3<'data>>();

    let results = iter0
        .zip(iter1)
        .zip(iter2)
        .zip(iter3)
        .map(|(((t0, t1), t2), t3)| match (t0, t1, t2, t3) {
            (Ok(t0), Ok(t1), Ok(t2), Ok(t3)) => op.evaluate(t0, t1, t2, t3),
            _ => op.evaluate_error(t0, t1, t2, t3),
        });

    let result = <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}
