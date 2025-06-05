use std::path::PathBuf;
use reqwest::Url;

/// Defines a requirement of preparing for a benchmark.
pub enum PrepRequirement {
    /// Requires that a file is downloaded at a given (relative) path.
    FileDownload {
        /// The URL that can be used to download the file.
        url: Url,
        /// The file name of the resulting file.
        file_name: PathBuf,
        /// An optional action that is applied to the downloaded file.
        action: Option<FileDownloadAction>
    },
}

/// Represents an action that is applied to a downloaded file.
pub enum FileDownloadAction {
    /// Unpacks a .bz2 file after its downloaded.
    UnpackBz2
}