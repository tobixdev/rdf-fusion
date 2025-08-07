use crate::memory::storage::log::builder::MemLogEntryBuilder;
use crate::memory::storage::log::content::{
    MemLogContent, MemLogEntry, MemLogEntryAction,
};
use crate::memory::storage::log::VersionNumber;
use crate::memory::MemObjectIdMapping;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_model::{NamedOrBlankNodeRef, Quad};

/// Allows writing entries into the log.
pub struct MemLogWriter<'log> {
    /// A mutable reference to the log content. The writer has exclusive access to the log.
    content: &'log mut MemLogContent,
    /// A mapping between terms and object ids.
    object_id_mapping: &'log MemObjectIdMapping,
    /// The version number of the log when the writer was created.
    version_number: VersionNumber,
    /// The logs created by the writer.
    log_builder: MemLogEntryBuilder,
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
            log_builder: MemLogEntryBuilder::new(),
        }
    }

    /// Transactionally inserts quads into the log.
    pub fn insert_quads(&mut self, quads: &[Quad]) -> Result<usize, StorageError> {
        let mut inserted = 0;
        let mut seen_quads = self.content.compute_quads(self.version_number);

        for quad in quads {
            let encoded = self.object_id_mapping.encode_quad(quad.as_ref())?;

            if seen_quads.contains(&encoded) {
                continue;
            }
            seen_quads.insert(encoded.clone());
            inserted += 1;

            self.log_builder.append_insertion(&encoded)?;
        }

        Ok(inserted)
    }

    /// Inserts an empty named graph into the log.
    pub fn insert_named_graph(
        &mut self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        let object_id = self.object_id_mapping.encode_term_intern(graph_name);

        let existing = self
            .content
            .contains_named_graph(object_id, self.version_number);
        if existing {
            return Ok(false);
        }

        self.log_builder
            .action(MemLogEntryAction::CreateEmptyNamedGraph(object_id))?;
        Ok(true)
    }

    /// Returns the log array.
    pub fn into_log_entry(self) -> Result<Option<MemLogEntry>, StorageError> {
        let version_number = self.version_number.increment();
        Ok(self.log_builder.build(version_number)?)
    }
}
