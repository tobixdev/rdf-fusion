use thiserror::Error;

#[derive(Debug, Error)]
#[error("Could not create typed value encoding: {0}")]
pub enum TypedValueEncodingCreationError {
    #[error("The type family with id '{0}' was provided more than once.")]
    DuplicateTypeFamily(String),
}