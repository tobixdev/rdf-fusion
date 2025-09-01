use crate::plans::canonicalize_uuids;
use anyhow::Context;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;
use rdf_fusion::store::Store;
use rdf_fusion::{QueryExplanation, QueryOptions};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmExploreQueryName, ExploreUseCase, NumProducts,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;

#[tokio::test]
pub async fn optimized_logical_plan_bsbm_explore() {
    for_all_explanations(|name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized)"),
            canonicalize_uuids(&explanation.optimized_logical_plan.to_string())
        )
    })
    .await;
}

#[tokio::test]
pub async fn execution_plan_bsbm_explore() {
    for_all_explanations(|name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(
            format!("{name} (Execution Plan)"),
            canonicalize_uuids(&string)
        )
    })
    .await;
}

async fn for_all_explanations(assertion: impl Fn(String, QueryExplanation) -> ()) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);

    // Load the benchmark data and set max query count to one.
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N1_000, None).unwrap();
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = Store::new_with_datafusion_config(
        SessionConfig::new().with_target_partitions(1),
        RuntimeEnv::default().into(),
    );
    for query_name in BsbmExploreQueryName::list_queries() {
        let benchmark_name = format!("BSBM Explore - {query_name}");
        let query =
            get_query_to_execute(benchmark.clone(), &benchmark_context, query_name);

        let (_, explanation) = store
            .explain_query_opt(query.text(), QueryOptions::default())
            .await
            .unwrap();

        assertion(benchmark_name, explanation);
    }
}

fn get_query_to_execute(
    benchmark: BsbmBenchmark<ExploreUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmExploreQueryName,
) -> SparqlRawOperation<BsbmExploreQueryName> {
    let query = benchmark
        .list_raw_operations(&benchmark_context)
        .context("Could not list raw operations for BSBM Explore benchmark. Have you prepared a bsbm-1000 dataset?")
        .unwrap()
        .into_iter()
        .find(|q| q.query_name() == query_name)
        .unwrap();

    println!("Executing query ({}): {}", query.query_name(), query.text());

    query
}
