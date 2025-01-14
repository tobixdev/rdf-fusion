use crate::sparql::error::SparqlEvaluationError;
use crate::sparql::{Query, QueryExplanation, QueryResults};
use datafusion::execution::SessionState;

pub struct QueryEvaluator {
    state: SessionState,
}

impl QueryEvaluator {
    #[must_use]
    pub fn new(state: SessionState) -> Self {
        Self { state }
    }

    pub fn execute(&self, query: &Query) -> Result<QueryResults, SparqlEvaluationError> {
        unimplemented!()
    }

    pub fn explain(
        &self,
        query: &Query,
    ) -> (
        Result<QueryResults, SparqlEvaluationError>,
        QueryExplanation,
    ) {
        unimplemented!("Sparql query evaluation not yet implemented")
    }
}
