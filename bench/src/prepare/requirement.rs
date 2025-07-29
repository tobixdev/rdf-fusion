use reqwest::Url;
use std::path::PathBuf;

/// Defines a requirement of preparing for a benchmark.
pub enum PrepRequirement {
    /// Requires that a file is downloaded at a given (relative) path.
    FileDownload {
        /// The URL that can be used to download the file.
        url: Url,
        /// The file name of the resulting file.
        file_name: PathBuf,
        /// An optional action that is applied to the downloaded file.
        action: Option<FileDownloadAction>,
    },
    /// Runs a closure.
    RunClosure {
        /// The closure to execute.
        execute: Box<dyn Fn() -> anyhow::Result<()>>,
        /// A checking function that can be used to check if the requirement is fulfilled.
        check_requirement: Box<dyn Fn() -> anyhow::Result<bool>>,
    },
    /// Runs a command.
    RunCommand {
        /// The working directory.
        workdir: PathBuf,
        /// The program to run.
        program: String,
        /// The args for the program.
        args: Vec<String>,
        /// A checking function that can be used to check if the requirement is fulfilled.
        check_requirement: Box<dyn Fn() -> anyhow::Result<bool>>,
    },
}

/// Represents an action that is applied to a downloaded file.
pub enum FileDownloadAction {
    /// Unpacks a file after it has been downloaded.
    Unpack(ArchiveType),
}

/// Represents the type of archive.
pub enum ArchiveType {
    /// A .bz2 archive.
    Bz2,
    /// A .zip archive.
    Zip,
}
