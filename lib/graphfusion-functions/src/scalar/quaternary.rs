use crate::dispatcher::SparqlOpDispatcher;
use crate::scalar::borrow_value;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::{FromArrow, ToArrow};
use graphfusion_functions_scalar::ReplaceSparqlOp;
use graphfusion_functions_scalar::{QuaternaryRdfTermValueOp, SparqlOp};

macro_rules! impl_quarternary_rdf_value_op {
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
                Ok(<$SPARQL_OP as QuaternaryRdfTermValueOp>::Result::encoded_datatype())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                dispatch_quaternary(&self.op, &args.args, args.number_rows)
            }
        }
    };
}

// Strings
impl_quarternary_rdf_value_op!(RegexValueQuarternaryDispatcher, ReplaceSparqlOp);

pub fn dispatch_quaternary<'data, TUdf>(
    udf: &TUdf,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: QuaternaryRdfTermValueOp,
    TUdf::Arg0<'data>: FromArrow<'data>,
    TUdf::Arg1<'data>: FromArrow<'data>,
    TUdf::Arg2<'data>: FromArrow<'data>,
    TUdf::Arg3<'data>: FromArrow<'data>,
    TUdf::Result<'data>: ToArrow,
{
    if args.len() != 4 {
        return exec_err!("Unexpected number of arguments.");
    }

    #[allow(
        clippy::missing_asserts_for_indexing,
        reason = "Already checked and not performance critical"
    )]
    let results = (0..number_of_rows).map(|i| {
        let arg0 = borrow_value::<TUdf::Arg0<'data>>(&args[0], i);
        let arg1 = borrow_value::<TUdf::Arg1<'data>>(&args[1], i);
        let arg2 = borrow_value::<TUdf::Arg2<'data>>(&args[2], i);
        let arg3 = borrow_value::<TUdf::Arg3<'data>>(&args[3], i);
        match (arg0, arg1, arg2, arg3) {
            (Ok(arg0), Ok(arg1), Ok(arg2), Ok(arg3)) => udf.evaluate(arg0, arg1, arg2, arg3),
            _ => udf.evaluate_error(),
        }
    });

    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
