use crate::builtin::BuiltinName;
use crate::{DFResult, FunctionName};
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use graphfusion_encoding::EncodingName;
use graphfusion_model::Term;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// A SPARQL operation that can be dispatched on arrow-encoded terms or term values.
pub trait GraphFusionUdfFactory: Debug + Send + Sync {
    /// Returns the name of the GraphFusion function.
    fn name(&self) -> FunctionName;

    /// Returns the encodings supported by the SPARQL operation. The encoding applies to both,
    /// inputs and the output.
    fn encoding(&self) -> Vec<EncodingName>;

    /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
    fn create_with_args(&self, constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>>;
}


/// A SPARQL aggregate operation that can be dispatched on arrow-encoded terms or term values.
pub trait GraphFusionUdafFactory: Debug + Send + Sync {
    /// Returns the name of the GraphFusion built-in.
    fn name(&self) -> FunctionName;

    /// Returns the encodings supported by the SPARQL operation. The encoding applies to both,
    /// inputs and the output.
    fn encoding(&self) -> Vec<EncodingName>;

    /// Creates a DataFusion [AggregateUDF] given the `constant_args`.
    fn create_with_args(&self, constant_args: HashMap<String, Term>) -> DFResult<Arc<AggregateUDF>>;
}