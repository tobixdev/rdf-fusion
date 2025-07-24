use crate::functions::name::FunctionName;
use crate::functions::RdfFusionFunctionArgs;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingName;
use std::fmt::Debug;
use std::sync::Arc;

/// A reference-counted pointer to an implementation of the `RdfFusionFunctionRegistry` trait.
///
/// This type alias is used throughout the codebase to pass around references to
/// function registries without tying code to specific implementations.
pub type RdfFusionFunctionRegistryRef = Arc<dyn RdfFusionFunctionRegistry>;

/// A registry for SPARQL functions that can create DataFusion UDFs and UDAFs.
///
/// This trait defines the interface for creating DataFusion user-defined functions
/// (UDFs) and user-defined aggregate functions (UDAFs) that implement SPARQL
/// function semantics.
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Functions](https://www.w3.org/TR/sparql11-query/#SparqlOps)
pub trait RdfFusionFunctionRegistry: Debug + Send + Sync {
    /// Returns the encodings supported by `function_name`.
    fn supported_encodings(&self, function_name: FunctionName) -> DFResult<Vec<EncodingName>>;

    /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
    fn create_udf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<ScalarUDF>>;

    /// Creates a DataFusion [AggregateUDF] given the `constant_args`.
    fn create_udaf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<AggregateUDF>>;
}
