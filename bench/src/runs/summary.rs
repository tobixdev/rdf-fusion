use std::fmt::{Display, Formatter};
use std::time::Duration;

/// Holds the statistics computed over multiple benchmark runs.
#[derive(Debug)]
pub struct BenchmarkRunsSummary {
    /// Represents how often the benchmark was executed.
    pub number_of_samples: u32,
    /// The average duration over all benchmark runs.
    pub avg_duration: Duration,
}

impl Display for BenchmarkRunsSummary {
    #[allow(clippy::use_debug)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Benchmark Summary:")?;
        writeln!(f, "Number of Samples: {}", self.number_of_samples)?;
        write!(f, "Average Duration: {:?}", self.avg_duration)
    }
}
