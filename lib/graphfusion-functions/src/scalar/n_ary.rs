use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::{FromArrow, ToArrow};
use graphfusion_functions_scalar::SparqlOp;
use graphfusion_functions_scalar::{CoalesceSparqlOp, ConcatSparqlOp, NAryRdfTermValueOp};
use crate::scalar::borrow_value;

macro_rules! impl_n_ary_rdf_value_op {
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
                Ok(<$SPARQL_OP as NAryRdfTermValueOp>::Result::encoded_datatype())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                dispatch_n_ary(&self.op, args.args.as_slice(), args.number_rows)
            }
        }
    };
}

// Functional Forms
impl_n_ary_rdf_value_op!(CoalesceValueNAryDispatcher, CoalesceSparqlOp);

// Strings
impl_n_ary_rdf_value_op!(ConcatValueNAryDispatcher, ConcatSparqlOp);

fn dispatch_n_ary<'data, TOp>(
    op: &TOp,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: NAryRdfTermValueOp,
    TOp::Args<'data>: FromArrow<'data>,
    TOp::Result<'data>: ToArrow,
{
    let results = (0..number_of_rows).map(|i| {
        let args = args
            .iter()
            .map(|a| borrow_value::<TOp::Args<'data>>(a, i))
            .collect::<Vec<_>>();

        if args.iter().all(Result::is_ok) {
            let args = args.into_iter().map(|arg| arg.unwrap()).collect::<Vec<_>>();
            op.evaluate(args.as_slice())
        } else {
            op.evaluate_error(args.as_slice())
        }
    });
    let result = TOp::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
