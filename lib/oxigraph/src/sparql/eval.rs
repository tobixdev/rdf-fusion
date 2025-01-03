use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{QueryExplanation, QueryResults};
use spargebra::Query;

#[derive(Clone, Default)]
pub struct QueryEvaluator {}

impl QueryEvaluator {
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
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
