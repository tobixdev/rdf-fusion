use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("Error while updating index.")]
pub enum IndexUpdateError {
    #[error("The updated index version number is not the expected one.")]
    UnexpectedVersionNumber,
}

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("Error while scanning the index.")]
pub enum IndexScanError {
    #[error("The version number of the index is already past the expected one.")]
    UnexpectedIndexVersionNumber,
}

#[derive(Debug, Error)]
#[error("Duplicate indexed component given.")]
pub struct IndexComponentsCreationError;
