mod parquet;

use crate::memory::storage::MemQuadStorageSnapshot;
use crate::memory::MemQuadStorage;
use rdf_fusion_encoding::QuadStorageEncoding;
use thiserror::Error;

/// Errors that can occur when persisting the in-memory storage.
#[derive(Debug, Error, Clone)]
#[error("Could not persist the in-memory storage: {0}")]
pub enum MemStoragePersistenceError {
    #[error("Unsupported encoding: {0}")]
    UnsupportedEncoding(QuadStorageEncoding),
    #[error("The given in-memory storage was invalid: {}")]
    InvalidMemQuadStorage(String),
}

/// Options for persisting the in-memory storage.
pub struct MemQuadPersistenceOptions {
    /// The encoding used for persisting the quad storage.
    encoding: QuadStorageEncoding,
}

/// Implements persistence for the in-memory storage.
///
/// While there may be an implementation of [MemQuadStoragePersistence] for a particular file
/// format, there is no guarantee that the exported files will be compatible with any [QuadStorage]
/// that is directly based on the file format.
pub trait MemQuadStoragePersistence {
    /// Exports the entire `storage` to persistent storage.
    async fn export(
        storage: &MemQuadStorageSnapshot,
        options: MemQuadPersistenceOptions,
    ) -> Result<(), MemStoragePersistenceError>;
}
