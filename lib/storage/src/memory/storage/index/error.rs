use crate::memory::storage::VersionNumber;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("Error while updating index.")]
pub enum IndexUpdateError {
    UnexpectedVersionNumber(UnexpectedVersionNumberError),
}

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("Error while scanning the index.")]
pub enum IndexScanError {
    UnexpectedVersionNumber(UnexpectedVersionNumberError),
}

#[derive(Debug, Error, PartialEq, Eq, Hash)]
#[error("The version number of the index {0} is not the expected one {1}.")]
pub struct UnexpectedVersionNumberError(pub VersionNumber, pub VersionNumber);

#[derive(Debug, Error)]
#[error("Duplicate indexed component given.")]
pub struct IndexComponentsCreationError;
