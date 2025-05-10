use crate::builtin::BuiltinName;
use crate::DFResult;
use datafusion::logical_expr::ScalarUDF;
use graphfusion_encoding::EncodingName;
use graphfusion_model::Term;
use std::collections::HashMap;
use std::fmt::Debug;

/// A SPARQL operation that can be dispatched on arrow-encoded terms or term values.
///
/// Contrary to a regular [SparqlOp], the SparqlOpDispatcher knows about the used encoding.
pub trait GraphFusionBuiltinFactory: Debug + Send + Sync {
    /// Returns the name of the GraphFusion built-in.
    fn name(&self) -> BuiltinName;

    /// Returns the encodings supported by the SPARQL operation. The encoding applies to both,
    /// inputs and the output.
    fn encoding(&self) -> Vec<EncodingName>;

    /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
    fn create_with_args(&self, constant_args: HashMap<String, Term>) -> DFResult<ScalarUDF>;
}
