use crate::plans::run_plan_assertions;
use anyhow::Context;
use datafusion::physical_plan::displayable;
use insta::assert_snapshot;
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions};
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{
    BsbmBenchmark, BsbmBusinessIntelligenceQueryName, BusinessIntelligenceUseCase,
    NumProducts,
};
use rdf_fusion_bench::environment::{BenchmarkContext, RdfFusionBenchContext};
use rdf_fusion_bench::operation::SparqlRawOperation;
use std::path::PathBuf;

#[tokio::test]
pub async fn optimized_logical_plan_bsbm_business_intelligence() {
    for_all_explanations(|name, explanation| {
        assert_snapshot!(
            format!("{name} (Optimized)"),
            &explanation.optimized_logical_plan.to_string()
        )
    })
    .await;
}

#[tokio::test]
pub async fn execution_plan_bsbm_business_intelligence() {
    for_all_explanations(|name, explanation| {
        let string = displayable(explanation.execution_plan.as_ref())
            .indent(false)
            .to_string();
        assert_snapshot!(format!("{name} (Execution Plan)"), &string)
    })
    .await;
}

async fn for_all_explanations(assertion: impl Fn(String, QueryExplanation) -> ()) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);

    // Load the benchmark data and set max query count to one.
    let benchmark =
        BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(NumProducts::N1_000, None)
            .unwrap();
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = benchmark.prepare_store(&benchmark_context).await.unwrap();
    for query_name in BsbmBusinessIntelligenceQueryName::list_queries() {
        let benchmark_name = format!("BSBM Business Intelligence - {query_name}");
        let query =
            get_query_to_execute(benchmark.clone(), &benchmark_context, query_name);

        let (_, explanation) = store
            .explain_query_opt(query.text(), QueryOptions::default())
            .await
            .unwrap();

        run_plan_assertions(|| assertion(benchmark_name, explanation));
    }
}

fn get_query_to_execute(
    benchmark: BsbmBenchmark<BusinessIntelligenceUseCase>,
    benchmark_context: &BenchmarkContext,
    query_name: BsbmBusinessIntelligenceQueryName,
) -> SparqlRawOperation<BsbmBusinessIntelligenceQueryName> {
    benchmark
        .list_raw_operations(&benchmark_context)
        .context("Could not list raw operations for BSBM Business Intelligence benchmark. Have you prepared a bsbm-1000 dataset?")
        .unwrap()
        .into_iter()
        .find(|q| q.query_name() == query_name)
        .unwrap()
}
