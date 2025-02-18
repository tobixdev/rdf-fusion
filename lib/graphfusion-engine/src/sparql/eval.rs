use crate::sparql::error::EvaluationError;
use crate::sparql::{Query, QueryExplanation, QueryOptions, QueryResults, QuerySolutionStream};
use datafusion::execution::SessionState;
use datafusion::prelude::{DataFrame, SessionContext};
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use std::sync::Arc;
use arrow_rdf::TABLE_QUADS;
use crate::sparql::rewriting::GraphPatternRewriter;

pub async fn evaluate_query(
    ctx: &SessionContext,
    query: &Query,
    _options: QueryOptions,
) -> Result<(QueryResults, Option<QueryExplanation>), EvaluationError> {
    match &query.inner {
        spargebra::Query::Select { pattern, .. } => {
            let quads = ctx.table_provider(TABLE_QUADS).await?;
            
            let rewriter = GraphPatternRewriter::new(quads);
            let logical_plan = rewriter.rewrite(pattern).map_err(|e| e.context("Cannot rewrite SPARQL query"))?;
            let dataframe = DataFrame::new(ctx.state(), logical_plan);
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
