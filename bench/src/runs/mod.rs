mod run;
mod summary;

pub use crate::runs::summary::BenchmarkRunsSummary;
use anyhow::Context;
pub use run::*;
use std::time::Duration;

/// Contains the runs of a single benchmark.
#[derive(Debug)]
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

    /// Summarizes the [BenchmarkRuns].
    ///
    /// See [BenchmarkRunsSummary] for a list of computed metrics.
    ///
    /// # Errors
    ///
    /// This operation may fail if there are too many samples.
    pub fn summarize(&self) -> anyhow::Result<BenchmarkRunsSummary> {
        let number_of_samples = u32::try_from(self.runs.len())
            .context("Too many samples for computing the average duration")?;
        let avg_duration = self.runs.iter().map(|run| run.duration).sum::<Duration>()
            / number_of_samples;
        Ok(BenchmarkRunsSummary {
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
