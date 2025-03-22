use crate::sparql::error::EvaluationError;
use crate::sparql::rewriting::GraphPatternRewriter;
use crate::sparql::{Query, QueryDataset, QueryExplanation, QueryOptions, QueryResults, QuerySolutionStream, QueryTripleStream};
use arrow_rdf::TABLE_QUADS;
use datafusion::prelude::{DataFrame, SessionContext};
use oxiri::Iri;
use oxrdf::Variable;
use spargebra::algebra::GraphPattern;
use std::sync::Arc;

pub async fn evaluate_query(
    ctx: &SessionContext,
    query: &Query,
    _options: QueryOptions,
) -> Result<(QueryResults, Option<QueryExplanation>), EvaluationError> {
    match &query.inner {
        spargebra::Query::Select {
            pattern, base_iri, ..
        } => {
            let dataframe = create_dataframe(ctx, &query.dataset, pattern, base_iri).await?;
            let batch_record_stream = dataframe.execute_stream().await?;
            let stream = QuerySolutionStream::new(create_variables(pattern), batch_record_stream);

            Ok((QueryResults::Solutions(stream), None))
        }
        spargebra::Query::Construct {
            template,
            pattern,
            base_iri,
            ..
        } => {
            let dataframe = create_dataframe(ctx, &query.dataset, pattern, base_iri).await?;
            let batch_record_stream = dataframe.execute_stream().await?;
            let stream = QuerySolutionStream::new(create_variables(pattern), batch_record_stream);

            Ok((
                QueryResults::Graph(QueryTripleStream::new(template.clone(), stream)),
                None,
            ))
        }
        spargebra::Query::Ask {
            pattern, base_iri, ..
        } => {
            let dataframe = create_dataframe(ctx, &query.dataset, pattern, base_iri).await?;
            let count = dataframe.limit(0, Some(1))?.count().await?;
            Ok((QueryResults::Boolean(count > 0), None))
        }
        _ => Err(EvaluationError::NotImplemented(String::from(
            "Query form not implemented",
        ))),
    }
}

async fn create_dataframe(
    ctx: &SessionContext,
    dataset: &QueryDataset,
    pattern: &GraphPattern,
    base_iri: &Option<Iri<String>>,
) -> Result<DataFrame, EvaluationError> {
    let quads = ctx.table_provider(TABLE_QUADS).await?;
    let logical_plan = GraphPatternRewriter::new(dataset.clone(), base_iri.clone(), quads)
        .rewrite(pattern)
        .map_err(|e| e.context("Cannot rewrite SPARQL query"))?;

    Ok(DataFrame::new(ctx.state(), logical_plan.clone()))
}

fn create_variables(graph_pattern: &GraphPattern) -> Arc<[Variable]> {
    let mut variables = Vec::new();
    graph_pattern.on_in_scope_variable(|v| variables.push(v.clone()));
    variables.into()
}
