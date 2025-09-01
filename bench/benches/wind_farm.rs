//! Runs the queries from the Wind Farm Benchmark.

mod utils;

use crate::utils::consume_results;
use crate::utils::verbose::{is_verbose, print_query_details};
use anyhow::Context;
use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use rdf_fusion::QueryOptions;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::windfarm::{
    NumTurbines, WindFarmBenchmark, WindFarmQueryName, get_wind_farm_raw_sparql_operation,
};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;
use tokio::runtime::{Builder, Runtime};

fn wind_farm_16_1_partition(c: &mut Criterion) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    wind_farm_16(c, &benchmarking_context);
}

fn wind_farm_16_4_partitions(c: &mut Criterion) {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 4);
    wind_farm_16(c, &benchmarking_context);
}

fn wind_farm_16(c: &mut Criterion, benchmarking_context: &RdfFusionBenchContext) {
    let verbose = is_verbose();
    let runtime = create_runtime();
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

    for query_name in WindFarmQueryName::list_queries() {
        let benchmark_name = format!(
            "Wind Farm 16 (target_partitions={}) - {query_name}",
            benchmarking_context.options().target_partitions.unwrap()
        );
        let query =
            get_wind_farm_raw_sparql_operation(&benchmark_context, query_name).unwrap();

        if verbose {
            runtime
                .block_on(print_query_details(
                    &store,
                    QueryOptions::default(),
                    &query_name.to_string(),
                    query.text(),
                ))
                .unwrap();
        }

        c.bench_function(&benchmark_name, |b| {
            b.to_async(&runtime).iter(|| async {
                let result = store
                    .query_opt(query.text(), QueryOptions::default())
                    .await
                    .unwrap();
                consume_results(result).await.unwrap();
            });
        });
    }
}

criterion_group!(
    name = wind_farm;
    config = Criterion::default().sample_size(10);
    targets = wind_farm_16_1_partition, wind_farm_16_4_partitions
);
criterion_main!(wind_farm);

fn create_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}
