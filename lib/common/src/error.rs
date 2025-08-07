use datafusion::error::DataFusionError;
use std::error::Error;
use std::io;

/// An error related to storage operations (reads, writes...).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Error from the OS I/O layer.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Error related to data corruption.
    #[error(transparent)]
    Corruption(#[from] CorruptionError),
    #[error("{0}")]
    Other(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl From<StorageError> for io::Error {
    #[inline]
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Io(error) => error,
            StorageError::Corruption(error) => error.into(),
            StorageError::Other(error) => Self::other(error),
        }
    }
}

// TODO: Improve when implementing proper error handling
impl From<DataFusionError> for StorageError {
    #[inline]
    fn from(error: DataFusionError) -> Self {
        Self::Other(Box::new(error))
    }
}

/// An error return if some content in the database is corrupted.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct CorruptionError(#[from] CorruptionErrorKind);

/// An error return if some content in the database is corrupted.
#[derive(Debug, thiserror::Error)]
enum CorruptionErrorKind {
    #[error("{0}")]
    Msg(String),
    #[error("{0}")]
    Other(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl CorruptionError {
    /// Builds an error from a printable error message.
    #[inline]
    pub fn new(error: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        Self(CorruptionErrorKind::Other(error.into()))
    }

    /// Builds an error from a printable error message.
    #[inline]
    pub fn msg(msg: impl Into<String>) -> Self {
        Self(CorruptionErrorKind::Msg(msg.into()))
    }
}

impl From<CorruptionError> for io::Error {
    #[inline]
    fn from(error: CorruptionError) -> Self {
        Self::new(io::ErrorKind::InvalidData, error)
    }
}
