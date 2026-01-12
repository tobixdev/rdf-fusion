use crate::prepare::PrepRequirement;
use async_trait::async_trait;
use std::path::Path;

pub mod bsbm;
mod name;
pub mod windfarm;

use crate::environment::BenchmarkContext;
use crate::report::BenchmarkReport;
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
    fn requirements(&self, bench_files_path: &Path) -> Vec<PrepRequirement>;

    /// Executes the benchmark using the given `bencher`.
    async fn execute(
        &self,
        bench_context: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Box<dyn BenchmarkReport>>;
}
