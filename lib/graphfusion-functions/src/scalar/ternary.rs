use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::{TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::SparqlOp;
use graphfusion_functions_scalar::{
    IfSparqlOp, RegexSparqlOp, ReplaceSparqlOp, SubStrSparqlOp, TernaryRdfTermValueOp,
};

macro_rules! impl_ternary_rdf_value_op {
    ($STRUCT_NAME: ident, $SPARQL_OP: ty) => {
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
                Ok(<$SPARQL_OP as TernaryRdfTermValueOp>::Result::encoded_datatype())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                dispatch_ternary(&self.op, &args.args, args.number_rows)
            }
        }
    };
}

// Functional Forms
impl_ternary_rdf_value_op!(IfValueTernaryDispatcher, IfSparqlOp);

// Strings
impl_ternary_rdf_value_op!(RegexValueTernaryDispatcher, RegexSparqlOp);
impl_ternary_rdf_value_op!(ReplaceValueTernaryDispatcher, ReplaceSparqlOp);
impl_ternary_rdf_value_op!(SubStrTernaryDispatcher, SubStrSparqlOp);

fn dispatch_ternary<'data, TEncoding, TOp>(
    op: &TOp,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: TernaryRdfTermValueOp,
    TEncoding: TermEncoding,
    TEncoding: TermDecoder<'data, TOp::Arg0<'data>>,
    TEncoding: TermDecoder<'data, TOp::Arg1<'data>>,
    TEncoding: TermDecoder<'data, TOp::Arg2<'data>>,
    TEncoding: TermEncoder<'data, TOp::Result<'data>>,
{
    if args.len() != 3 {
        return exec_err!("Unexpected number of arguments.");
    }


todo!()
    // let arg0 = TermDecoder::decode_terms()
    // let results = (0..number_of_rows).map(|i| {
    //     let arg0 = borrow_value::<TOp::Arg0<'data>>(&args[0], i);
    //     let arg1 = borrow_value::<TOp::Arg1<'data>>(&args[1], i);
    //     let arg2 = borrow_value::<TOp::Arg2<'data>>(&args[2], i);
    //     match (arg0, arg1, arg2) {
    //         (Ok(arg0), Ok(arg1), Ok(arg2)) => op.evaluate(arg0, arg1, arg2),
    //         _ => op.evaluate_error(arg0, arg1, arg2),
    //     }
    // });
    // let result = TOp::Result::iter_into_array(results)?;
    // Ok(ColumnarValue::Array(result))
}
