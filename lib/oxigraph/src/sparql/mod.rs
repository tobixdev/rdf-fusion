//! [SPARQL](https://www.w3.org/TR/sparql11-overview/) implementation.
//!
//! Stores execute SPARQL. See [`Store`](crate::store::Store::query()) for an example.

mod algebra;
mod error;
mod eval;
mod explanation;
mod http;
mod model;
pub mod results;
mod update;

pub use crate::sparql::algebra::{Query, QueryDataset, Update};
pub use crate::sparql::error::{
    EvaluationError, LoaderError, QueryEvaluationError, SerializerError, StorageError,
};
pub use crate::sparql::eval::QueryEvaluator;
pub use crate::sparql::explanation::QueryExplanation;
pub use crate::sparql::model::{QueryResults, QuerySolution, QuerySolutionIter, QueryTripleIter};
pub use oxrdf::{Variable, VariableNameParseError};
pub use spargebra::SparqlSyntaxError;

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn evaluate_query(
    query: impl TryInto<Query, Error = impl Into<EvaluationError>>,
    options: QueryOptions,
) -> Result<(Result<QueryResults, EvaluationError>, QueryExplanation), EvaluationError> {
    let query = query.try_into().map_err(Into::into)?;
    let mut evaluator = options.into_evaluator();
    let (results, explanation) = evaluator.explain(&query.inner);
    let results = results.map_err(Into::into);
    Ok((results, explanation))
}

/// Options for SPARQL query evaluation.
///
///
/// If the `"http-client"` optional feature is enabled,
/// a simple HTTP 1.1 client is used to execute [SPARQL 1.1 Federated Query](https://www.w3.org/TR/sparql11-federated-query/) SERVICE calls.
///
/// Usage example disabling the federated query support:
/// ```
/// use oxigraph::sparql::QueryOptions;
/// use oxigraph::store::Store;
///
/// let store = Store::new()?;
/// store.query_opt(
///     "SELECT * WHERE { SERVICE <https://query.wikidata.org/sparql> {} }",
///     QueryOptions::default().without_service_handler(),
/// )?;
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
#[derive(Clone)]
pub struct QueryOptions {}

impl QueryOptions {
    fn into_evaluator(mut self) -> QueryEvaluator {
        QueryEvaluator::new()
    }
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {}
    }
}

/// Options for SPARQL update evaluation.
#[derive(Clone, Default)]
pub struct UpdateOptions {
    query_options: QueryOptions,
}

impl From<QueryOptions> for UpdateOptions {
    #[inline]
    fn from(query_options: QueryOptions) -> Self {
        Self { query_options }
    }
}
