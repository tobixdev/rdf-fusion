use crate::sparql::error::EvaluationError;
use crate::sparql::rewriter::SparqlToDataFusionRewriter;
use crate::sparql::{Query, QueryExplanation, QueryOptions, QueryResults, QuerySolutionStream};
use datafusion::execution::SessionState;
use datafusion::prelude::DataFrame;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use std::sync::Arc;

pub async fn evaluate_query(
    state: SessionState,
    query: &Query,
    _options: QueryOptions,
) -> Result<(QueryResults, Option<QueryExplanation>), EvaluationError> {
    match &query.inner {
        spargebra::Query::Select { pattern, .. } => {
            let rewriter = SparqlToDataFusionRewriter::new(&state);
            let logical_plan = rewriter.rewrite(pattern).map_err(|e| e.context("Cannot rewrite SPARQL query"))?;
            let dataframe = DataFrame::new(state, logical_plan);
            let batch_record_stream = dataframe.execute_stream().await?;
            let stream = QuerySolutionStream::new(create_variables(pattern), batch_record_stream);
            Ok((QueryResults::Solutions(stream), None))
        }
        _ => Err(EvaluationError::NotImplemented(String::from(
            "Query form not implemented",
        ))),
    }
}

fn create_variables(graph_pattern: &GraphPattern) -> Arc<[Variable]> {
    let mut variables = Vec::new();
    graph_pattern.on_in_scope_variable(|v| variables.push(v.clone()));
    variables.into()
}
