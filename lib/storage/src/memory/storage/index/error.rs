use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("Error while updating index.")]
pub enum IndexUpdateError {
    #[error("Error while deleting triple from index.")]
    IndexDeletionError(#[from] IndexDeletionError),
}

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("Error while deleting triple from index.")]
pub enum IndexDeletionError {
    #[error("Triple does not exist in index.")]
    NonExistingTriple,
}

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("Error while scanning the index.")]
pub enum IndexScanError {
    #[error("The version number of the index is already past the expected one.")]
    UnexpectedIndexVersionNumber,
}
