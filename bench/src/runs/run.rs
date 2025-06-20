use pprof::Report;
use std::time::Duration;

/// Represents a single run of a benchmark.
#[derive(Debug)]
pub struct BenchmarkRun {
    /// The duration of the benchmark run.
    pub duration: Duration,
    /// The profiling report of the benchmark run.
    pub report: Option<Report>,
}

impl Clone for BenchmarkRun {
    fn clone(&self) -> Self {
        Self {
            duration: self.duration,
            report: self.report.as_ref().map(|r| Report {
                data: r.data.clone(),
                timing: r.timing.clone(),
            }),
        }
    }
}
