use crate::sparql::error::QueryEvaluationError;
use crate::sparql::rewriting::GraphPatternRewriter;
use crate::sparql::{
    Query, QueryDataset, QueryExplanation, QueryOptions, QueryResults, QuerySolutionStream,
    QueryTripleStream,
};
use datafusion::common::plan_err;
use datafusion::prelude::{DataFrame, SessionContext};
use rdf_fusion_functions::registry::RdfFusionFunctionRegistryRef;
use rdf_fusion_model::Iri;
use rdf_fusion_model::Variable;
use spargebra::algebra::GraphPattern;
use std::sync::Arc;

pub async fn evaluate_query(
    ctx: &SessionContext,
    registry: RdfFusionFunctionRegistryRef,
    query: &Query,
    _options: QueryOptions,
) -> Result<(QueryResults, Option<QueryExplanation>), QueryEvaluationError> {
    match &query.inner {
        spargebra::Query::Select {
            pattern, base_iri, ..
        } => {
            let dataframe =
                create_dataframe(ctx, registry, &query.dataset, pattern, base_iri).await?;
            let variables = create_variables(&dataframe);
            let batch_record_stream = dataframe.execute_stream().await?;
            let stream = QuerySolutionStream::new(variables, batch_record_stream);

            Ok((QueryResults::Solutions(stream), None))
        }
        spargebra::Query::Construct {
            template,
            pattern,
            base_iri,
            ..
        } => {
            let dataframe =
                create_dataframe(ctx, registry, &query.dataset, pattern, base_iri).await?;
            let variables = create_variables(&dataframe);
            let batch_record_stream = dataframe.execute_stream().await?;
            let stream = QuerySolutionStream::new(variables, batch_record_stream);

            Ok((
                QueryResults::Graph(QueryTripleStream::new(template.clone(), stream)),
                None,
            ))
        }
        spargebra::Query::Ask {
            pattern, base_iri, ..
        } => {
            let dataframe =
                create_dataframe(ctx, registry, &query.dataset, pattern, base_iri).await?;
            let count = dataframe.limit(0, Some(1))?.count().await?;
            Ok((QueryResults::Boolean(count > 0), None))
        }
        spargebra::Query::Describe { .. } => Err(QueryEvaluationError::NotImplemented(
            String::from("Query form not implemented"),
        )),
    }
}

async fn create_dataframe(
    ctx: &SessionContext,
    registry: RdfFusionFunctionRegistryRef,
    dataset: &QueryDataset,
    pattern: &GraphPattern,
    base_iri: &Option<Iri<String>>,
) -> Result<DataFrame, QueryEvaluationError> {
    let logical_plan = GraphPatternRewriter::new(registry, dataset.clone(), base_iri.clone())
        .rewrite(pattern)
        .map_err(|e| e.context("Cannot rewrite SPARQL query"))?;

    Ok(DataFrame::new(ctx.state(), logical_plan))
}

#[allow(clippy::expect_used)]
fn create_variables(data_frame: &DataFrame) -> Arc<[Variable]> {
    data_frame
        .schema()
        .fields()
        .iter()
        .map(|f| Variable::new(f.name()).expect("Variables already checked."))
        .collect::<Vec<_>>()
        .into()
}
