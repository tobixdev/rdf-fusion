use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::time::Duration;

/// Contains the runs of a single benchmark.
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkRuns {
    /// The individual runs.
    runs: Vec<BenchmarkRun>,
}

impl BenchmarkRuns {
    /// Creates a new empty [BenchmarkRuns].
    pub fn new() -> Self {
        Self { runs: Vec::new() }
    }

    /// Adds a new [BenchmarkRun].
    pub fn add_run(&mut self, run: BenchmarkRun) {
        self.runs.push(run);
    }

    pub fn summarize(&self) -> anyhow::Result<BenchmarkSummary> {
        let number_of_samples = u32::try_from(self.runs.len())
            .context("Too many samples for computing the average duration")?;
        let avg_duration =
            self.runs.iter().map(|run| run.duration).sum::<Duration>() / number_of_samples;

        Ok(BenchmarkSummary {
            number_of_samples,
            avg_duration,
        })
    }
}

impl Default for BenchmarkRuns {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a single run of a benchmark.
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkRun {
    /// The duration of the benchmark run.
    pub duration: Duration,
}

/// Holds the statistics computed over [BenchmarkRuns].
#[derive(Debug, Serialize, Deserialize)]
pub struct BenchmarkSummary {
    /// Represents how often the benchmark was executed.
    pub number_of_samples: u32,
    /// The average duration over all benchmark runs.
    pub avg_duration: Duration,
}

impl Display for BenchmarkSummary {
    #[allow(clippy::use_debug)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Benchmark Summary:")?;
        writeln!(f, "Number of Samples: {}", self.number_of_samples)?;
        write!(f, "Average Duration: {:?}", self.avg_duration)
    }
}
