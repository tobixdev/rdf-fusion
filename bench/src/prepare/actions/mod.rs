use crate::prepare::actions::unpack::unpack_archive;
use anyhow::Error;
use std::path::Path;

mod unpack;

/// Represents an action that is applied to a downloaded file.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileAction {
    /// Unpacks a file after it has been downloaded.
    Unpack(ArchiveType),
}

/// Represents the type of archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ArchiveType {
    /// A .bz2 archive.
    Bz2,
    /// A .zip archive.
    Zip,
}

/// Executes the given `action` on the given file.
pub fn execute_file_action(
    file_path: &Path,
    action: Option<&FileAction>,
) -> Result<(), Error> {
    match action {
        None => Ok(()),
        Some(FileAction::Unpack(archive_type)) => {
            unpack_archive(file_path, *archive_type)
        }
    }
}
