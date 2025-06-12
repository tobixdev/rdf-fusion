use std::path::Path;

/// Represents the final report of executing a benchmark.
pub trait BenchmarkReport {
    /// Writes the results of this benchmark to the given directory.
    fn write_results(&self, output_dir: &Path) -> anyhow::Result<()>;
}
