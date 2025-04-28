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
///
///
/// If the `"http-client"` optional feature is enabled,
/// a simple HTTP 1.1 client is used to execute [SPARQL 1.1 Federated Query](https://www.w3.org/TR/sparql11-federated-query/) SERVICE calls.
///
/// Usage example disabling the federated query support:
/// ```
/// use graphfusion::sparql::QueryOptions;
/// use graphfusion::store::Store;
///
/// let store = Store::new()?;
/// store.query_opt(
///     "SELECT * WHERE { SERVICE <https://query.wikidata.org/sparql> {} }",
///     QueryOptions::default().without_service_handler(),
/// )?;
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
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
