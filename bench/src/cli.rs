use crate::benchmarks::BsbmDatasetSize;
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(about, version, name = "rdf-fusion-bench")]
/// RdfFusion command line toolkit and SPARQL HTTP server
pub struct Args {
    /// Indicates whether the benchmark should be prepared or executed.
    pub operation: Operation,
    /// Indicates which benchmark should be executed.
    #[clap(subcommand)]
    pub benchmark: BenchmarkName,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum Operation {
    /// Prepares the data for a given benchmarks.
    Prepare,
    /// Executes a given benchmarks, assuming that the preparation has already been done.
    Execute,
}

#[derive(Subcommand)]
pub enum BenchmarkName {
    /// Runs a BSBM instance with already generated queries from Oxigraph.
    Bsbm {
        /// Indicates the scaling of the dataset.
        #[arg(short, long, default_value = "1000")]
        dataset_size: BsbmDatasetSize,
        /// Provides an upper bound on the number of queries to be executed.
        #[arg(short, long)]
        query_size: Option<u64>,
    },
}
