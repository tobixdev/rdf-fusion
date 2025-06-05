use crate::benchmarks::{Benchmark, BsbmDatasetSize, BsbmExploreBenchmark};
use crate::cli::{Args, BenchmarkName, Operation};
use crate::environment::BenchmarkingContext;
use clap::Parser;
use std::path::PathBuf;

mod benchmarks;
mod cli;
mod environment;
mod opeartions;
mod prepare;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = Args::parse();

    let data = PathBuf::from("./data");
    let context = BenchmarkingContext::new(data);

    let benchmark = create_benchmark_instance(&matches)?;

    match matches.operation {
        Operation::Prepare => {
            for requirement in benchmark.requirements() {
                context.prepare_requirement(requirement).await?;
            }
        }
        Operation::Execute => {
            println!("Executing benchmark {} ... ", benchmark.name());

            println!("Verifying requirements ...");
            for requirement in benchmark.requirements() {
                context.ensure_requirement(requirement)?;
            }
            println!("🎉 Requirements verified");

            println!("Executing benchmark ...");
            benchmark.execute(&context).await?;
            println!("🎉 Benchmark done");
        }
    }

    Ok(())
}

fn create_benchmark_instance(args: &Args) -> anyhow::Result<Box<dyn Benchmark>> {
    match args.benchmark {
        BenchmarkName::Bsbm {
            dataset_size,
            query_size,
        } => {
            let dataset_size = BsbmDatasetSize::try_from(dataset_size)?;
            Ok(Box::new(BsbmExploreBenchmark::new(
                dataset_size,
                query_size,
            )))
        }
    }
}
