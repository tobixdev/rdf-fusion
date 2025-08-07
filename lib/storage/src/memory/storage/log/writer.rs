use crate::memory::storage::log::content::{
    MemLogArray, MemLogArrayBuilder, MemLogContent,
};
use crate::memory::storage::log::VersionNumber;
use crate::memory::MemObjectIdMapping;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_model::Quad;

/// Allows writing entries into the log.
pub struct MemLogWriter<'log> {
    /// A mutable reference to the log content. The writer has exclusive access to the log.
    content: &'log mut MemLogContent,
    /// A mapping between terms and object ids.
    object_id_mapping: &'log MemObjectIdMapping,
    /// The version number of the log when the writer was created.
    version_number: VersionNumber,
    /// The logs created by the writer.
    log_builder: MemLogArrayBuilder,
}

impl<'log> MemLogWriter<'log> {
    /// Creates a new [MemLogWriter].
    pub fn new(
        log: &'log mut MemLogContent,
        object_id_mapping: &'log MemObjectIdMapping,
        version_number: VersionNumber,
    ) -> Self {
        MemLogWriter {
            content: log,
            object_id_mapping,
            version_number,
            log_builder: MemLogArrayBuilder::new(),
        }
    }

    /// Transactionally inserts quads into the log.
    pub fn insert_quads(&mut self, quads: &[Quad]) -> Result<usize, StorageError> {
        for quad in quads {
            let encoded = self.object_id_mapping.encode_quad(quad.as_ref())?;
            // TODO: duplicates
            self.log_builder.append_insertion(
                encoded.graph_name,
                encoded.subject,
                encoded.predicate,
                encoded.object,
            );
        }

        Ok(quads.len())
    }

    /// Returns the log array.
    pub fn into_log_array(self) -> Result<MemLogArray, StorageError> {
        let version_number = self.version_number.increment();
        Ok(self.log_builder.build(version_number)?)
    }
}
