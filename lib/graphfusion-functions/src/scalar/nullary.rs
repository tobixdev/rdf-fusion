use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use graphfusion_encoding::ToArrow;
use graphfusion_functions_scalar::{
    BNodeSparqlOp, NullarySparqlOp, RandSparqlOp, SparqlOp, StrUuidSparqlOp, UuidSparqlOp,
};
use std::fmt::Debug;

macro_rules! impl_nullary_op {
    ($STRUCT_NAME:ident, $SPARQL_OP:ty) => {
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
                Ok(<$SPARQL_OP as NullarySparqlOp>::Result::encoded_datatype())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                let results = (0..args.number_rows)
                    .into_iter()
                    .map(|_| self.op.evaluate());
                let result = <$SPARQL_OP as NullarySparqlOp>::Result::iter_into_array(results)?;
                Ok(ColumnarValue::Array(result))
            }
        }
    };
}

impl_nullary_op!(BNodeNullary, BNodeSparqlOp);
impl_nullary_op!(RandNullary, RandSparqlOp);
impl_nullary_op!(StrUuidNullary, StrUuidSparqlOp);
impl_nullary_op!(UuidNullary, UuidSparqlOp);
