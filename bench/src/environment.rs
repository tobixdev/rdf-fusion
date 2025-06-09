use crate::benchmarks::BenchmarkName;
use crate::prepare::PrepRequirement;
use crate::prepare::{ensure_file_download, prepare_file_download};
use crate::results::{BenchmarkResults, BenchmarkRun};
use anyhow::{bail, Context};
use std::fs;
use std::fs::File;
use std::future::Future;
use std::path::{Path, PathBuf};
use tokio::time::Instant;

/// Represents a context used to execute benchmarks.
pub struct BenchmarkingContext {
    /// The path to the data dir.
    data_dir: PathBuf,
    /// The path to the current directory. This might be different from the `data_dir` if a
    /// benchmark is running.
    results_dir: PathBuf,
    /// The results of the benchmark run.
    results: BenchmarkResults,
}

impl BenchmarkingContext {
    /// Creates a new [BenchmarkingContext].
    pub fn new(data_dir: PathBuf, results_dir: PathBuf) -> Self {
        Self {
            data_dir,
            results_dir,
            results: BenchmarkResults::new(),
        }
    }

    /// Returns the results of the benchmark run.
    pub fn results(&self) -> &BenchmarkResults {
        &self.results
    }

    /// Resolves a relative path `file` against the data directory.
    pub fn join_data_dir(&self, file: &Path) -> anyhow::Result<PathBuf> {
        if !file.is_relative() {
            bail!("Only relative paths can be resolved.")
        }

        Ok(self.data_dir.join(file))
    }

    /// Prepares the context such that `requirement` is fulfilled.
    pub async fn prepare_requirement(&self, requirement: PrepRequirement) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::FileDownload {
                url,
                file_name,
                action,
            } => prepare_file_download(self, url, file_name, action).await,
        }
    }

    /// Ensures that the `requirement` is fulfilled in this context.
    pub fn ensure_requirement(&self, requirement: PrepRequirement) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::FileDownload { file_name, .. } => {
                ensure_file_download(self, file_name.as_path())
            }
        }
    }

    /// Writes the results of a particular benchmark to disk.
    pub fn write_results_of(&self, benchmark_name: BenchmarkName) -> anyhow::Result<()> {
        let results = self
            .results
            .get_runs(benchmark_name)
            .context("Failed to get runs from Benchmark")?;

        let data_json_path = self.results_dir.join("data.json");
        let data_json_file = File::create(data_json_path)?;
        serde_json::to_writer_pretty(data_json_file, results)?;

        let summary_path = self.results_dir.join("summary.txt");
        fs::write(summary_path, results.summarize()?.to_string())?;

        Ok(())
    }

    /// Creates a new folder in the results directory and uses it until [Self::pop_results_dir] is
    /// called.
    ///
    /// This can be used to create folder hierarchies to separate the results of different
    /// benchmarks.
    #[allow(clippy::create_dir)]
    pub fn push_results_dir(&mut self, dir: &str) -> anyhow::Result<()> {
        self.results_dir.push(dir);
        if self.results_dir.exists() {
            println!(
                "Cleaning results directory '{}' ...",
                self.results_dir.as_path().display()
            );
            fs::remove_dir_all(self.results_dir.as_path())?;
        }
        fs::create_dir(self.results_dir.as_path())?;
        Ok(())
    }

    /// Pops the last directory from the stack.
    pub fn pop_results_dir(&mut self) {
        self.results_dir.pop();
    }

    /// Creates a new bencher and modifies the context for this benchmark.
    pub fn create_bencher(&mut self, benchmark_name: BenchmarkName) -> anyhow::Result<Bencher<'_>> {
        self.push_results_dir(&benchmark_name.dir_name())?;
        Ok(Bencher {
            context: self,
            benchmark_name,
        })
    }
}

/// A benchmarker that can be used to execute benchmarks.
///
/// It holds a reference to the current context to store its results.
pub struct Bencher<'ctx> {
    /// Reference to the benchmarking context.
    context: &'ctx mut BenchmarkingContext,
    /// Name of the benchmark that is being executed.
    benchmark_name: BenchmarkName,
}

impl<'ctx> Bencher<'ctx> {
    /// Returns a reference to the benchmarking context.
    pub fn context(&self) -> &BenchmarkingContext {
        self.context
    }

    /// Returns the name of the benchmark that is being executed.
    pub fn benchmark_name(&self) -> BenchmarkName {
        self.benchmark_name
    }

    /// Benchmarks the given `body`.
    pub async fn bench<F, R, O>(&mut self, body: F) -> anyhow::Result<O>
    where
        F: Fn() -> R,
        R: Future<Output = anyhow::Result<O>>,
    {
        println!("Warming up for 5 seconds...");
        let warmup_start = Instant::now();
        while Instant::now() - warmup_start < std::time::Duration::from_secs(5) {
            body().await?;
        }
        println!("Warmup done.");

        println!("Benching...");
        let start = Instant::now();
        let result = body().await?;
        let end = Instant::now();
        println!("Benching done.");

        let duration = end - start;
        let run = BenchmarkRun { duration };
        self.context.results.add_run(self.benchmark_name, run);

        Ok(result)
    }

    /// Instructs the benchmarking context to write the results of this benchmark to disk.
    pub fn write_results(&self) -> anyhow::Result<()> {
        self.context.write_results_of(self.benchmark_name)
    }
}

/// Pops the results directory from the context when the bencher is dropped.
impl Drop for Bencher<'_> {
    fn drop(&mut self) {
        self.context.pop_results_dir();
    }
}
