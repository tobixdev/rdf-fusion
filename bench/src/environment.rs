use crate::BenchmarkingOptions;
use crate::benchmarks::BenchmarkName;
use crate::prepare::{PrepRequirement, prepare_run_closure, prepare_run_command};
use crate::prepare::{ensure_file_download, prepare_file_download};
use anyhow::bail;
use std::fs;
use std::path::{Path, PathBuf};

/// Represents a context used to execute benchmarks.
pub struct RdfFusionBenchContext {
    /// General options for the benchmarks.
    options: BenchmarkingOptions,
    /// The path to the data dir.
    data_dir: PathBuf,
    /// The path to the current directory. This might be different from the `data_dir` if a
    /// benchmark is running.
    results_dir: PathBuf,
}

impl RdfFusionBenchContext {
    /// Creates a new [RdfFusionBenchContext].
    pub fn new(
        options: BenchmarkingOptions,
        data_dir: PathBuf,
        results_dir: PathBuf,
    ) -> Self {
        Self {
            options,
            data_dir,
            results_dir,
        }
    }

    /// Returns the [BenchmarkingOptions] for this context.
    pub fn options(&self) -> &BenchmarkingOptions {
        &self.options
    }

    /// Resolves a relative path `file` against the data directory.
    pub fn join_data_dir(&self, file: &Path) -> anyhow::Result<PathBuf> {
        if !file.is_relative() {
            bail!("Only relative paths can be resolved.")
        }

        Ok(self.data_dir.join(file))
    }

    /// Prepares the context such that `requirement` is fulfilled.
    pub async fn prepare_requirement(
        &self,
        requirement: PrepRequirement,
    ) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::FileDownload {
                url,
                file_name,
                action,
            } => prepare_file_download(self, url, file_name, action).await,
            PrepRequirement::RunClosure { execute, .. } => {
                prepare_run_closure(self, &execute)
            }
            PrepRequirement::RunCommand {
                workdir,
                program,
                args,
                ..
            } => {
                let workdir = self.join_data_dir(&workdir)?;
                prepare_run_command(&workdir, &program, &args)
            }
        }
    }

    /// Ensures that the `requirement` is fulfilled in this context.
    pub fn ensure_requirement(&self, requirement: PrepRequirement) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::FileDownload { file_name, .. } => {
                ensure_file_download(self, file_name.as_path())
            }
            PrepRequirement::RunClosure {
                check_requirement, ..
            }
            | PrepRequirement::RunCommand {
                check_requirement, ..
            } => check_requirement(self),
        }
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
    pub fn create_benchmark_context(
        &mut self,
        benchmark_name: BenchmarkName,
    ) -> anyhow::Result<BenchmarkContext<'_>> {
        self.push_results_dir(&benchmark_name.dir_name())?;
        Ok(BenchmarkContext {
            context: self,
            benchmark_name,
        })
    }
}

/// A benchmarker that can be used to execute benchmarks.
///
/// It holds a reference to the current context to store its results.
pub struct BenchmarkContext<'ctx> {
    /// Reference to the benchmarking context.
    context: &'ctx mut RdfFusionBenchContext,
    /// Name of the benchmark that is being executed.
    benchmark_name: BenchmarkName,
}

impl<'ctx> BenchmarkContext<'ctx> {
    /// Returns a reference to the benchmarking context.
    pub fn parent(&self) -> &RdfFusionBenchContext {
        self.context
    }

    /// Returns the name of the benchmark that is being executed.
    pub fn benchmark_name(&self) -> BenchmarkName {
        self.benchmark_name
    }

    /// Returns the path to the results directory of this benchmark.
    pub fn results_dir(&self) -> &Path {
        self.context.results_dir.as_path()
    }
}

/// Pops the results directory from the context when the bencher is dropped.
impl Drop for BenchmarkContext<'_> {
    fn drop(&mut self) {
        self.context.pop_results_dir();
    }
}
