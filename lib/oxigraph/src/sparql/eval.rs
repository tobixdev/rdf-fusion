use datafusion::execution::SessionState;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{Query, QueryExplanation, QueryResults};

pub struct QueryEvaluator {
    state: SessionState,
}

impl QueryEvaluator {
    #[must_use]
    pub fn new(state: SessionState) -> Self {
        Self{ state }
    }

    pub fn execute(&self, query: &Query) -> Result<QueryResults, QueryEvaluationError> {
        unimplemented!()
    }

    pub fn explain(
        &self,
        query: &Query,
    ) -> (Result<QueryResults, QueryEvaluationError>, QueryExplanation) {
        unimplemented!("Sparql query evaluation not yet implemented")
    }
}
