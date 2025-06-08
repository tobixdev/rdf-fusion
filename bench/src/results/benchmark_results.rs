use crate::benchmarks::BenchmarkName;
use crate::results::run::BenchmarkRuns;
use crate::results::BenchmarkRun;
use std::collections::HashMap;

/// Represents the overall results of running (multiple) benchmarks.
pub struct BenchmarkResults {
    /// A mapping from the executed benchmark to all recorded runs.
    runs: HashMap<BenchmarkName, BenchmarkRuns>,
}

impl BenchmarkResults {
    /// Returns a new empty [BenchmarkResults].
    pub fn new() -> Self {
        Self {
            runs: HashMap::new(),
        }
    }

    /// Returns the runs of a given benchmark.
    pub fn get_runs(&self, name: BenchmarkName) -> Option<&BenchmarkRuns> {
        self.runs.get(&name)
    }

    /// Adds a new run for a given `namer
    pub fn add_run(&mut self, name: BenchmarkName, run: BenchmarkRun) {
        let runs = self.runs.entry(name).or_insert(BenchmarkRuns::new());
        runs.add_run(run);
    }
}
