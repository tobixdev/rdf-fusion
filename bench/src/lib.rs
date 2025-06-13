#![allow(clippy::print_stdout)]

use crate::benchmarks::bsbm::BsbmExploreBenchmark;
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
mod utils;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum Operation {
    /// Prepares the data for a given benchmark.
    Prepare,
    /// Executes a given benchmark, assuming that the preparation has already been done.
    Execute,
}

/// TODO
pub async fn execute_benchmark_operation(
    operation: Operation,
    benchmark: BenchmarkName,
) -> anyhow::Result<()> {
    let data = PathBuf::from("./data");
    let results = PathBuf::from("./results");
    fs::create_dir_all(&results)?;

    let mut context = RdfFusionBenchContext::new(data, results);

    let benchmark = create_benchmark_instance(benchmark);
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

fn create_benchmark_instance(benchmark: BenchmarkName) -> Box<dyn Benchmark> {
    match benchmark {
        BenchmarkName::Bsbm {
            dataset_size,
            max_query_count: query_size,
        } => Box::new(BsbmExploreBenchmark::new(dataset_size, query_size)),
    }
}
