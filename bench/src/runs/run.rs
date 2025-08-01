use std::time::Duration;

/// Represents a single run of a benchmark.
#[derive(Debug)]
pub struct BenchmarkRun {
    /// The duration of the benchmark run.
    pub duration: Duration,
}

impl Clone for BenchmarkRun {
    fn clone(&self) -> Self {
        Self {
            duration: self.duration,
        }
    }
}
