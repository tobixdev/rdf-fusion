//! Evaluates certain quad patterns against the BSBM dataset.

use anyhow::Context;
use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use rdf_fusion::QueryResults;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::benchmarks::windfarm::{NumTurbines, WindFarmBenchmark};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;
use tokio::runtime::{Builder, Runtime};

fn storage_bsbm(c: &mut Criterion) {
    let runtime = create_runtime();
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"));
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N10_000, None).unwrap();
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&benchmark_context))
        .context("
    Failed to prepare store. Have you downloaded the data?

    Execute `just prepare-benches` for downloading the data. Then, run the benchmark from the `bench` directory.
    ")
        .unwrap();

    let patterns = [
        (
            "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> <http://www.w3.org/2000/01/rdf-schema#label> ?label",
            1,
        ),
        (
            "?otherProduct <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/productFeature> ?otherFeature",
            212342,
        ),
        (
            "?product <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1>",
            10000,
        ),
    ];

    for (pattern, results) in patterns {
        c.bench_function(format!("BSBM Pattern: << {pattern} >>").as_str(), |b| {
            b.to_async(&runtime).iter(|| async {
                let query = String::from("SELECT * { ") + pattern + " }";
                let result = store.query(&query).await.unwrap();
                assert_number_of_results(result, results).await;
            });
        });
    }
}

fn storage_wind_farm(c: &mut Criterion) {
    let runtime = create_runtime();
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"));

    let benchmark = WindFarmBenchmark::new(NumTurbines::N16);
    let benchmark_name = benchmark.name();
    let benchmark_context = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = runtime
        .block_on(benchmark.prepare_store(&benchmark_context))
        .context("
    Failed to prepare store. Have you downloaded the data?

    Execute `just prepare-benches` for downloading the data. Then, run the benchmark from the `bench` directory.
    ")
        .unwrap();

    let patterns = [(
        "?dp <https://github.com/magbak/chrontext#hasValue> ?val",
        1632960,
    )];

    for (pattern, results) in patterns {
        c.bench_function(
            format!("Wind Farm Pattern: << {pattern} >>").as_str(),
            |b| {
                b.to_async(&runtime).iter(|| async {
                    let query = String::from("SELECT * { ") + pattern + ". }";
                    let result = store.query(&query).await.unwrap();
                    assert_number_of_results(result, results).await;
                });
            },
        );
    }
}

criterion_group!(
    name = quad_pattern_matching;
    config = Criterion::default().sample_size(10);
    targets =  storage_bsbm, storage_wind_farm
);
criterion_main!(quad_pattern_matching);

fn create_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

async fn assert_number_of_results(result: QueryResults, n: usize) {
    match result {
        QueryResults::Solutions(mut solutions) => {
            let mut count = 0;
            while let Some(sol) = solutions.next().await {
                sol.unwrap();
                count += 1;
            }
            assert_eq!(count, n);
        }
        _ => panic!("Unexpected QueryResults"),
    }
}
