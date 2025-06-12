use crate::benchmarks::bsbm::BsbmDatasetSize;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize, Subcommand)]
pub enum BenchmarkName {
    /// Runs a BSBM instance with already generated queries from Oxigraph.
    Bsbm {
        /// Indicates the scaling of the dataset.
        #[arg(short, long, default_value = "1000")]
        dataset_size: BsbmDatasetSize,
        /// Provides an upper bound on the number of queries to be executed.
        #[arg(short, long)]
        max_query_count: Option<u64>,
    },
}

impl BenchmarkName {
    /// Returns a directory name for the benchmark.
    pub fn dir_name(&self) -> String {
        match self {
            BenchmarkName::Bsbm {
                dataset_size,
                max_query_count,
            } => match max_query_count {
                Some(max_query_count) => format!("bsbm-{dataset_size}-{max_query_count}"),
                None => format!("bsbm-{dataset_size}"),
            },
        }
    }
}

impl Display for BenchmarkName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkName::Bsbm {
                dataset_size,
                max_query_count,
            } => match max_query_count {
                Some(max_query_count) => write!(
                    f,
                    "BSBM Explore: dataset_size={dataset_size}, max_query_count={max_query_count}"
                ),
                None => write!(f, "BSBM Explore: dataset_size={dataset_size}"),
            },
        }
    }
}
