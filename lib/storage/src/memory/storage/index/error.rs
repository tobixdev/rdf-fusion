use thiserror::Error;

#[derive(Debug, Error)]
#[error("Duplicate indexed component given.")]
pub struct IndexComponentsCreationError;
