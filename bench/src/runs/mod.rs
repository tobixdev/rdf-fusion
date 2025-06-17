mod run;
mod summary;

use crate::runs::summary::BenchmarkRunsSummary;
use anyhow::Context;
use pprof::Frames;
pub use run::*;
use std::collections::HashMap;
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
        let avg_duration =
            self.runs.iter().map(|run| run.duration).sum::<Duration>() / number_of_samples;
        Ok(BenchmarkRunsSummary {
            number_of_samples,
            avg_duration,
        })
    }

    /// Accumulates the profiles of the individual runs.
    ///
    /// The result is a mapping from a [Frames] (i.e., stack trace) to a count. The higher the
    /// count, the more often a stack configuration has been observed. This information can then
    /// be plotted in a flamegraph.
    ///
    /// The goal is to obtain a single profile for multiple executions of the same benchmark.
    pub fn accumulate_profiles(&self) -> anyhow::Result<HashMap<Frames, isize>> {
        let mut result = HashMap::new();

        for report in self.runs.iter().filter_map(|r| r.report.as_ref()) {
            for (frame, count) in &report.data {
                let existing = result.get_mut(frame);
                match existing {
                    None => {
                        result.insert(frame.clone(), *count);
                    }
                    Some(old_count) => {
                        let new_count = (*old_count).checked_add(*count).ok_or_else(|| {
                            anyhow::anyhow!("Overflow occurred when accumulating profile data")
                        })?;
                        *old_count = new_count;
                    }
                }
            }
        }

        Ok(result)
    }
}

impl Default for BenchmarkRuns {
    fn default() -> Self {
        Self::new()
    }
}
