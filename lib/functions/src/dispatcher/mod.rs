use std::fmt::Debug;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use crate::DFResult;

mod overloaded;

/// A SPARQL operation that can be dispatched on arrow-encoded terms or term values.
///
/// Contrary to a regular [SparqlOp], the SparqlOpDispatcher knows about the used encoding.
pub trait SparqlOpDispatcher: Debug {
    /// Returns the name of the SPARQL operation.
    fn name(&self) -> &str;

    /// Returns the signature of the operation. This function is called for SPARQL operations when
    /// integrating them into DataFusion.
    fn signature(&self) -> &Signature;

    /// Returns the return type of the operation. This function is called for SPARQL operations when
    /// integrating them into DataFusion.
    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType>;

    /// Invokes the actual SPARQL operation on the given `args`.
    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue>;
}
