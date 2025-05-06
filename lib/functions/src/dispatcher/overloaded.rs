use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use std::sync::Arc;
use crate::DFResult;
use crate::dispatcher::SparqlOpDispatcher;

#[derive(Debug)]
struct OverloadedSparqlOpDispatcher {
    name: String,
    inner: Vec<Arc<dyn SparqlOpDispatcher>>,
}

impl OverloadedSparqlOpDispatcher {}

impl SparqlOpDispatcher for OverloadedSparqlOpDispatcher {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        todo!()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        todo!()
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        todo!()
    }
}
