#![allow(clippy::print_stdout)]

use crate::benchmarks::bsbm::{
    BsbmBenchmark, BusinessIntelligenceUseCase, ExploreUseCase,
};
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::RdfFusionBenchContext;
use clap::ValueEnum;
use std::fs;
use std::path::PathBuf;

pub mod benchmarks;
mod environment;
mod prepare;
mod report;
mod runs;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum Operation {
    /// Prepares the data for a given benchmark.
    Prepare,
    /// Executes a given benchmark, assuming that the preparation has already been done.
    Execute,
}

/// Provides options for the benchmarking process.
pub struct BenchmarkingOptions {
    /// Indicates whether the benchmarking results should be verbose.
    ///
    /// For example, while non-verbose results could show an aggregated version of multiple runs,
    /// verbose results could write the results for each run.
    pub verbose_results: bool,
}

/// Executes an `operation` of a given `benchmark`.
///
/// - [Operation::Prepare] prepares the data for the given benchmark.
/// - [Operation::Execute] executes the given benchmark. The runner verifies the requirements before
///   executing the benchmark (e.g., whether a file exists).
pub async fn execute_benchmark_operation(
    options: BenchmarkingOptions,
    operation: Operation,
    benchmark: BenchmarkName,
) -> anyhow::Result<()> {
    let data = PathBuf::from(format!("./data/{}", benchmark.dir_name()));
    let results = PathBuf::from("./results");
    fs::create_dir_all(&results)?;
    let mut context = RdfFusionBenchContext::new(options, data, results);

    let benchmark = create_benchmark_instance(benchmark)?;
    match operation {
        Operation::Prepare => {
            println!("Preparing benchmark '{}' ...", benchmark.name());

            for requirement in benchmark.requirements() {
                context.prepare_requirement(requirement).await?;
            }

            println!("Benchmark '{}' prepared.\n", benchmark.name());
        }
        Operation::Execute => {
            println!("Executing benchmark '{}' ...\n", benchmark.name());

            println!("Verifying requirements ...");
            for requirement in benchmark.requirements() {
                context.ensure_requirement(requirement)?;
            }
            println!("Requirements verified\n");

            println!("Executing benchmark ...");
            {
                let bench_context = context.create_benchmark_context(benchmark.name())?;
                let report = benchmark.execute(&bench_context).await?;
                report.write_results(bench_context.results_dir())?;
            }
            println!("Benchmark '{}' done\n", benchmark.name());
        }
    }
    Ok(())
}

fn create_benchmark_instance(
    benchmark: BenchmarkName,
) -> anyhow::Result<Box<dyn Benchmark>> {
    let benchmark: Box<dyn Benchmark> = match benchmark {
        BenchmarkName::BsbmExplore {
            num_products: dataset_size,
            max_query_count: query_size,
        } => Box::new(BsbmBenchmark::<ExploreUseCase>::try_new(
            dataset_size,
            query_size,
        )?),
        BenchmarkName::BsbmBusinessIntelligence {
            num_products: dataset_size,
            max_query_count: query_size,
        } => Box::new(BsbmBenchmark::<BusinessIntelligenceUseCase>::try_new(
            dataset_size,
            query_size,
        )?),
    };
    Ok(benchmark)
}
