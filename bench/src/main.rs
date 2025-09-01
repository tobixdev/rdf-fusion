use clap::Parser;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::{execute_benchmark_operation, BenchmarkingOptions, Operation};

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RdfFusionBenchArgs::parse();

    let options = BenchmarkingOptions {
        verbose_results: args.verbose_results,
        target_partitions: args.target_partitions,
        memory_size: args.memory_limit.map(|val| 1024 * 1024 * val),
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
    /// Defines how many target partitions DataFusion should use.
    #[arg(short, long)]
    pub target_partitions: Option<usize>,
    /// Defines how much memory DataFusion is allowed to use. In MiB.
    #[arg(short, long)]
    pub memory_limit: Option<usize>,
    /// Indicates which benchmark should be executed.
    #[clap(subcommand)]
    pub benchmark: BenchmarkName,
}
