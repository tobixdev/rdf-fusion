use crate::memory::persistence::{
    MemQuadPersistenceOptions, MemQuadStoragePersistence, MemStoragePersistenceError,
};
use crate::memory::storage::MemQuadStorageSnapshot;
use rdf_fusion_encoding::QuadStorageEncoding;
use std::path::PathBuf;

/// An implementation of [MemQuadStoragePersistence] for the Parquet file format.
///
/// A single Parquet file is used to store the entire quad storage.
///
/// # Limitations
///
/// Currently, only the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding) is
/// supported. A translation of an object id-based [MemQuadStorageSnapshot] is supported.
pub struct ParquetMemQuadStoragePersistence {
    /// The path of the Parquet file.
    file_path: PathBuf,
}

impl MemQuadStoragePersistence for ParquetMemQuadStoragePersistence {
    async fn export(
        storage: &MemQuadStorageSnapshot,
        options: &MemQuadPersistenceOptions,
    ) -> Result<(), MemStoragePersistenceError> {
        if options.encoding != QuadStorageEncoding::PlainTerm {
            return Err(MemStoragePersistenceError::UnsupportedEncoding(
                options.encoding.clone(),
            ));
        };

        todo!("Stream all quads, Use existing translation, Write File")
    }
}
