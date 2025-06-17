use clap::Parser;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::{execute_benchmark_operation, BenchmarkingOptions, Operation};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RdfFusionBenchArgs::parse();

    let options = BenchmarkingOptions {
        verbose_results: args.verbose_results,
    };
    execute_benchmark_operation(options, args.operation, args.benchmark).await?;
    Ok(())
}

#[derive(Parser)]
#[command(about, version, name = "rdf-fusion-bench")]
/// RdfFusion command line toolkit and SPARQL HTTP server
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
