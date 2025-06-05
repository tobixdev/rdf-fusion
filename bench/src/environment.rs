use crate::prepare::PrepRequirement;
use crate::prepare::{ensure_file_download, prepare_file_download};
use anyhow::bail;
use std::future::Future;
use std::path::{Path, PathBuf};

/// Represents a context used to execute benchmarks.
pub struct BenchmarkingContext {
    /// The path to the data dir.
    data_dir: PathBuf,
}

impl BenchmarkingContext {
    /// Creates a new [BenchmarkingContext].
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
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
                ensure_file_download(self, file_name)
            }
        }
    }

    /// Benchmarks the given `body`.
    pub async fn bench<F, R, O>(&self, body: F) -> anyhow::Result<O>
    where
        F: Fn() -> R,
        R: Future<Output = anyhow::Result<O>>,
    {
        body().await
    }
}
