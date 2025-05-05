use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::{
    BNodeSparqlOp, NullarySparqlOp, RandSparqlOp, SparqlOp, StrUuidSparqlOp, UuidSparqlOp,
};
use std::fmt::Debug;

macro_rules! impl_nullary_op {
    ($ENCODING: ty, $STRUCT_NAME:ident, $SPARQL_OP:ty) => {
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
                let results = (0..args.number_rows)
                    .into_iter()
                    .map(|_| self.op.evaluate());
                let result = <$ENCODING as TermEncoder<_>>::encode_terms(results)?;
                Ok(ColumnarValue::Array(EncodingArray::into_array(result)))
            }
        }
    };
}

impl_nullary_op!(TermValueEncoding, BNodeTermValue, BNodeSparqlOp);
impl_nullary_op!(TermValueEncoding, RandTermValue, RandSparqlOp);
impl_nullary_op!(TermValueEncoding, StrUuidTermValue, StrUuidSparqlOp);
impl_nullary_op!(TermValueEncoding, UuidTermValue, UuidSparqlOp);
