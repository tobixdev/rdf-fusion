use crate::prepare::PrepRequirement;
use async_trait::async_trait;

mod bsbm_explore;
mod name;

use crate::environment::Bencher;
pub use bsbm_explore::{BsbmDatasetSize, BsbmExploreBenchmark};
pub use name::BenchmarkName;

/// Represents a benchmark.
#[async_trait]
pub trait Benchmark {
    /// Returns the id of the benchmark.
    ///
    /// This must be a valid folder name and will be used to store files / results on the file
    /// system.
    fn name(&self) -> BenchmarkName;

    /// Returns a list of preparation requirements.
    fn requirements(&self) -> Vec<PrepRequirement>;

    /// Executes the benchmark using the given `context`.
    async fn execute(&self, bencher: &mut Bencher<'_>) -> anyhow::Result<()>;
}
