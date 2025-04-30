//! [SPARQL](https://www.w3.org/TR/sparql11-overview/) implementation.
//!
//! Stores execute SPARQL. See [`Store`](graphfusion::store::Store::query()) for an example.

mod algebra;
pub mod error;
mod eval;
mod explanation;
mod rewriting;

pub use crate::results::{QueryResults, QuerySolution, QuerySolutionStream, QueryTripleStream};
pub use crate::sparql::algebra::{Query, QueryDataset, Update};
pub use crate::sparql::explanation::QueryExplanation;
pub use eval::evaluate_query;
pub use model::{Variable, VariableNameParseError};
pub use spargebra::SparqlSyntaxError;

/// Options for SPARQL query evaluation.
#[derive(Clone, Default)]
pub struct QueryOptions;

/// Options for SPARQL update evaluation.
#[derive(Clone, Default)]
pub struct UpdateOptions;

impl From<QueryOptions> for UpdateOptions {
    #[inline]
    fn from(_query_options: QueryOptions) -> Self {
        Self {}
    }
}
