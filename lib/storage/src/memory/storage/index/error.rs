use thiserror::Error;

#[derive(Debug, Error)]
#[error("Error while updating index.")]
pub enum IndexUpdateError {
    #[error("Error while deleting triple from index.")]
    IndexDeletionError(#[from] IndexDeletionError),
}

#[derive(Debug, Error)]
#[error("Error while deleting triple from index.")]
pub enum IndexDeletionError {
    #[error("Triple does not exist in index.")]
    NonExistingTriple,
}
