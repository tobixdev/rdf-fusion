#![allow(clippy::print_stdout)]

use crate::benchmarks::bsbm::BsbmExploreBenchmark;
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::RdfFusionBenchContext;
use clap::ValueEnum;
use futures::StreamExt;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion::{QueryOptions, QueryResults};
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

// #[tokio::test]
async fn test() {
    let store = load_bsbm_1000().await.unwrap();
    let (result, explanation) = store.explain_query_opt("
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>

            SELECT DISTINCT ?product ?productLabel WHERE {
                ?product rdfs:label ?productLabel .
                FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> != ?product)

                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productFeature ?prodFeature .
                ?product bsbm:productFeature ?prodFeature .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric1 ?origProperty1 .
                ?product bsbm:productPropertyNumeric1 ?simProperty1 .
                FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))

                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric2 ?origProperty2 .
                ?product bsbm:productPropertyNumeric2 ?simProperty2 .
                FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))
            }
            ORDER BY ?productLabel
            LIMIT 5
            ", QueryOptions::default()).await.unwrap();

    let expr = format!(
        "Explanation:\n Logical Plan: {}",
        &explanation.optimized_logical_plan
    );

    assert_number_of_results(result, 0).await;
}

async fn load_bsbm_1000() -> anyhow::Result<Store> {
    let data_path = PathBuf::from("./data/dataset-1000.nt");
    let data = fs::read(data_path)?;
    let memory_store = Store::new();
    memory_store
        .load_from_reader(RdfFormat::NTriples, data.as_slice())
        .await?;
    Ok(memory_store)
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
        QueryResults::Graph(mut triples) => {
            let mut count = 0;
            while let Some(sol) = triples.next().await {
                sol.unwrap();
                count += 1;
            }
            assert_eq!(count, n);
        }
        _ => panic!("Unexpected QueryResults"),
    }
}
