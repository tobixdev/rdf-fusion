#![allow(clippy::print_stdout)]

use crate::benchmarks::{Benchmark, BenchmarkName, BsbmExploreBenchmark};
use crate::environment::BenchmarkingContext;
use clap::ValueEnum;
use std::fs;
use std::path::PathBuf;

pub mod benchmarks;
mod environment;
mod operations;
mod prepare;
mod results;

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

    let mut context = BenchmarkingContext::new(data, results);

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
                let mut bencher = context.create_bencher(benchmark.name())?;
                benchmark.execute(&mut bencher).await?;
                bencher.write_results()?;
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
