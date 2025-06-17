use crate::sparql::error::QueryEvaluationError;
use crate::sparql::rewriting::GraphPatternRewriter;
use crate::sparql::{
    Query, QueryDataset, QueryExplanation, QueryOptions, QueryResults, QuerySolutionStream,
    QueryTripleStream,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::execution::SessionState;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use rdf_fusion_functions::registry::RdfFusionFunctionRegistryRef;
use rdf_fusion_model::Iri;
use rdf_fusion_model::Variable;
use spargebra::algebra::GraphPattern;
use std::sync::Arc;

/// TODO
pub async fn evaluate_query(
    ctx: &SessionContext,
    registry: RdfFusionFunctionRegistryRef,
    query: &Query,
    _options: QueryOptions,
) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
    match &query.inner {
        spargebra::Query::Select {
            pattern, base_iri, ..
        } => {
            let (stream, explanation) =
                graph_pattern_to_stream(ctx.state(), registry, query, pattern, base_iri).await?;
            Ok((QueryResults::Solutions(stream), explanation))
        }
        spargebra::Query::Construct {
            template,
            pattern,
            base_iri,
            ..
        } => {
            let (stream, explanation) =
                graph_pattern_to_stream(ctx.state(), registry, query, pattern, base_iri).await?;
            Ok((
                QueryResults::Graph(QueryTripleStream::new(template.clone(), stream)),
                explanation,
            ))
        }
        spargebra::Query::Ask {
            pattern, base_iri, ..
        } => {
            let (mut stream, explanation) =
                graph_pattern_to_stream(ctx.state(), registry, query, pattern, base_iri).await?;
            let count = stream.next().await;
            Ok((QueryResults::Boolean(count.is_some()), explanation))
        }
        spargebra::Query::Describe { .. } => Err(QueryEvaluationError::NotImplemented(
            String::from("Query form not implemented"),
        )),
    }
}

/// TODO
async fn graph_pattern_to_stream(
    state: SessionState,
    registry: RdfFusionFunctionRegistryRef,
    query: &Query,
    pattern: &GraphPattern,
    base_iri: &Option<Iri<String>>,
) -> Result<(QuerySolutionStream, QueryExplanation), QueryEvaluationError> {
    let task = state.task_ctx();

    let (execution_plan, explanation) =
        create_execution_plan(state, registry, &query.dataset, pattern, base_iri).await?;
    let variables = create_variables(&execution_plan.schema());

    let batch_record_stream = execute_stream(execution_plan, task)?;
    let stream = QuerySolutionStream::new(variables, batch_record_stream);
    Ok((stream, explanation))
}

/// TODO
async fn create_execution_plan(
    state: SessionState,
    registry: RdfFusionFunctionRegistryRef,
    dataset: &QueryDataset,
    pattern: &GraphPattern,
    base_iri: &Option<Iri<String>>,
) -> Result<(Arc<dyn ExecutionPlan>, QueryExplanation), QueryEvaluationError> {
    let planning_time_start = std::time::Instant::now();
    let logical_plan = GraphPatternRewriter::new(registry, dataset.clone(), base_iri.clone())
        .rewrite(pattern)
        .map_err(|e| e.context("Cannot rewrite SPARQL query"))?;
    let optimized_plan = state.optimize(&logical_plan)?;
    let physical_plan = state
        .query_planner()
        .create_physical_plan(&optimized_plan, &state)
        .await?;
    let planning_time = planning_time_start.elapsed();

    let explanation = QueryExplanation {
        planning_time,
        initial_logical_plan: logical_plan,
        optimized_logical_plan: optimized_plan,
        execution_plan: Arc::clone(&physical_plan),
    };
    Ok((Arc::clone(&physical_plan), explanation))
}

#[allow(clippy::expect_used)]
fn create_variables(schema: &Schema) -> Arc<[Variable]> {
    schema
        .fields()
        .iter()
        .map(|f| Variable::new(f.name()).expect("Variables already checked."))
        .collect::<Vec<_>>()
        .into()
}
