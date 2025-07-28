//! [SPARQL](https://www.w3.org/TR/sparql11-overview/) implementation.

mod algebra;
pub mod error;
mod eval;
mod explanation;
mod optimizer;
mod rewriting;

pub use crate::results::{
    QueryResults, QuerySolution, QuerySolutionStream, QueryTripleStream,
};
pub use crate::sparql::algebra::{Query, QueryDataset, Update};
pub use crate::sparql::explanation::QueryExplanation;
pub use eval::evaluate_query;
pub use rdf_fusion_model::{Variable, VariableNameParseError};
pub use spargebra::SparqlSyntaxError;

/// Defines how many optimizations the query optimizer should apply.
///
/// Currently, the default value is [OptimizationLevel::Full], as we are still searching for a
/// subset that performs well on many queries. Once this subset has been identified, the default
/// value will be [OptimizationLevel::Default].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OptimizationLevel {
    /// No optimizations, except rewrites that are necessary for a working query.
    None,
    /// A balanced default optimization level. Suitable for simple queries or those handling modest
    /// data volumes.
    Default,
    /// Runs all optimizations. Ideal for complex queries or those processing large datasets.
    #[default]
    Full,
}

/// Options for SPARQL query evaluation.
#[derive(Clone, Default)]
pub struct QueryOptions {
    /// The defined optimization level
    pub optimization_level: OptimizationLevel,
}

/// Options for SPARQL update evaluation.
#[derive(Clone, Default)]
pub struct UpdateOptions;

impl From<QueryOptions> for UpdateOptions {
    #[inline]
    fn from(_query_options: QueryOptions) -> Self {
        Self {}
    }
}
