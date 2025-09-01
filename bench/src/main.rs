use clap::Parser;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::{BenchmarkingOptions, Operation, execute_benchmark_operation};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RdfFusionBenchArgs::parse();

    let options = BenchmarkingOptions {
        verbose_results: args.verbose_results,
        target_partitions: None,
        memory_size: None,
    };
    execute_benchmark_operation(options, args.operation, args.benchmark).await?;
    Ok(())
}

#[derive(Parser)]
#[command(about, version, name = "rdf-fusion-bench")]
/// RDF Fusion command line toolkit and SPARQL HTTP server
pub struct RdfFusionBenchArgs {
    /// Indicates whether the benchmark should be prepared or executed.
    pub operation: Operation,
    /// Indicates whether the benchmark results should be verbose.
    #[arg(short, long, default_value = "false")]
    pub verbose_results: bool,
    /// Indicates which benchmark should be executed.
    #[clap(subcommand)]
    pub benchmark: BenchmarkName,
}
