use crate::plans::canonicalize_uuids;
use anyhow::Context;
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;
use rdf_fusion::{QueryExplanation, QueryOptions};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::windfarm::{
    NumTurbines, WindFarmBenchmark, WindFarmQueryName, get_wind_farm_raw_sparql_operation,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;

#[tokio::test]
pub async fn initial_logical_plan_wind_farm() {
    for_all_explanations(|name, explanation| {
        assert_snapshot!(
            format!("{name} (Initial)"),
            canonicalize_uuids(&explanation.initial_logical_plan.to_string())
        );

        assert_snapshot!(
            format!("{name} (Optimized)"),
            canonicalize_uuids(&explanation.optimized_logical_plan.to_string())
        );

        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(
            format!("{name} (Execution Plan)"),
            canonicalize_uuids(&string)
        );
    })
    .await;
}

async fn for_all_explanations(assertion: impl Fn(String, QueryExplanation) -> ()) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"));

    // Load the benchmark data and set max query count to one.
    let benchmark = WindFarmBenchmark::new(NumTurbines::N4);
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = benchmark.prepare_store(&benchmark_context).await.unwrap();
    for query_name in WindFarmQueryName::list_queries() {
        let benchmark_name = format!("Wind Farm - {query_name}");
        let query = get_query_to_execute(&benchmark_context, query_name);

        let (_, explanation) = store
            .explain_query_opt(query.text(), QueryOptions::default())
            .await
            .unwrap();

        assertion(benchmark_name, explanation);
    }
}

fn get_query_to_execute(
    benchmark_context: &BenchmarkContext,
    query_name: WindFarmQueryName,
) -> SparqlRawOperation<WindFarmQueryName> {
    get_wind_farm_raw_sparql_operation(benchmark_context, query_name)
        .context("Could not list raw operations for Wind Farm benchmark. Have you prepared a wind-farm-4 dataset?")
        .unwrap()
}
