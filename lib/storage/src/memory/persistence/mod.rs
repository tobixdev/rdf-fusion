mod parquet;

use crate::memory::storage::MemQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use rdf_fusion_encoding::QuadStorageEncoding;
use std::io::Write;
use thiserror::Error;

pub use parquet::ParquetMemQuadStoragePersistence;

/// Errors that can occur when persisting the in-memory storage.
#[derive(Debug, Error)]
#[error("Could not persist the in-memory storage. {0}")]
pub enum MemStoragePersistenceError {
    #[error("Unsupported encoding: {0}")]
    UnsupportedEncoding(QuadStorageEncoding),
    #[error("The given in-memory storage was invalid: {0}")]
    InvalidMemQuadStorage(String),
    #[error("Could not query the storage: {0}")]
    QueryError(#[from] DataFusionError),
    #[error("Error while writing files: {0}")]
    DataFileError(Box<dyn std::error::Error + Send + Sync>),
}

impl From<ParquetError> for MemStoragePersistenceError {
    fn from(err: ParquetError) -> Self {
        MemStoragePersistenceError::DataFileError(Box::new(err))
    }
}

/// Options for persisting the in-memory storage.
pub struct MemQuadPersistenceOptions {
    /// The encoding used for persisting the quad storage. If the encoding is not specified, the
    /// implementation may choose an encoding.
    encoding: Option<QuadStorageEncoding>,
}

impl MemQuadPersistenceOptions {
    /// Overrides the encoding used for the persistence.
    pub fn with_encoding(self, encoding: QuadStorageEncoding) -> Self {
        Self {
            encoding: Some(encoding),
        }
    }
}

impl Default for MemQuadPersistenceOptions {
    fn default() -> Self {
        Self { encoding: None }
    }
}

/// Implements persistence for the in-memory storage.
///
/// While there may be an implementation of [MemQuadStoragePersistence] for a particular file
/// format, there is no guarantee that the exported files will be compatible with any [QuadStorage]
/// that is directly based on the file format.
#[async_trait]
pub trait MemQuadStoragePersistence {
    /// The metadata that is returned once the file has been written.
    type Metadata;

    /// Exports the entire `storage` to persistent storage.
    async fn export<TWrite: Write + Send>(
        &self,
        write: TWrite,
        storage: &MemQuadStorageSnapshot,
        options: &MemQuadPersistenceOptions,
    ) -> Result<Self::Metadata, MemStoragePersistenceError>;
}
