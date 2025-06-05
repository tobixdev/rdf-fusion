use clap::Parser;
use rdf_fusion_bench::benchmarks::BenchmarkName;
use rdf_fusion_bench::{execute_benchmark_operation, Operation};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = RdfFusionBenchArgs::parse();
    execute_benchmark_operation(args.operation, args.benchmark).await?;
    Ok(())
}

#[derive(Parser)]
#[command(about, version, name = "rdf-fusion-bench")]
/// RdfFusion command line toolkit and SPARQL HTTP server
pub struct RdfFusionBenchArgs {
    /// Indicates whether the benchmark should be prepared or executed.
    pub operation: Operation,
    /// Indicates which benchmark should be executed.
    #[clap(subcommand)]
    pub benchmark: BenchmarkName,
}
