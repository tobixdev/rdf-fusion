use crate::memory::MemObjectIdMapping;
use crate::memory::storage::log::builder::MemLogEntryBuilder;
use crate::memory::storage::log::content::{
    ClearTarget, MemLogContent, MemLogEntry, MemLogEntryAction,
};
use rdf_fusion_common::error::StorageError;
use rdf_fusion_model::{GraphNameRef, NamedOrBlankNodeRef, Quad, QuadRef};
use crate::memory::storage::VersionNumber;

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
        let changes = self.content.compute_changes(self.version_number);
        let mut inserted = 0;
        let mut seen_quads = changes.map(|c| c.inserted).unwrap_or_default();

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

    /// Transactionally removes quads from the log.
    pub fn delete(&mut self, quads: &[QuadRef<'_>]) -> Result<usize, StorageError> {
        let mut deleted = 0;
        let mut seen_quads = self
            .content
            .compute_changes(self.version_number)
            .map(|c| c.inserted)
            .unwrap_or_default();

        for quad in quads {
            let encoded = self.object_id_mapping.encode_quad(*quad)?;

            if !seen_quads.contains(&encoded) {
                continue;
            }

            seen_quads.remove(&encoded);
            deleted += 1;
            self.log_builder.append_deletion(&encoded)?;
        }

        Ok(deleted)
    }

    /// Inserts an empty named graph into the log.
    pub fn insert_named_graph(
        &mut self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        let object_id = self.object_id_mapping.encode_term_intern(graph_name);

        if let Some(changes) = self.content.compute_changes(self.version_number) {
            let existing = changes.created_named_graphs.contains(&object_id)
                || changes
                    .inserted
                    .iter()
                    .any(|q| q.graph_name.0 == object_id);
            if existing {
                return Ok(false);
            }
        }

        self.log_builder
            .action(MemLogEntryAction::CreateNamedGraph(object_id))?;
        Ok(true)
    }

    /// Clears all graphs in the store.
    pub fn clear(&mut self) -> Result<(), StorageError> {
        self.log_builder
            .action(MemLogEntryAction::Clear(ClearTarget::AllGraphs))?;
        Ok(())
    }

    /// Clears a single graph in the store.
    pub fn clear_graph(
        &mut self,
        graph_name: GraphNameRef<'_>,
    ) -> Result<(), StorageError> {
        let object_id = self.object_id_mapping.encode_graph_name_intern(graph_name);
        self.log_builder
            .action(MemLogEntryAction::Clear(ClearTarget::Graph(object_id)))?;
        Ok(())
    }

    /// Drops a single named graph in the store.
    pub fn drop_named_graph(
        &mut self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        let object_id = self.object_id_mapping.encode_term_intern(graph_name);

        if let Some(changes) = self.content.compute_changes(self.version_number) {
            let existing = changes.created_named_graphs.contains(&object_id)
                || changes
                    .inserted
                    .iter()
                    .any(|q| q.graph_name.0 == object_id);
            if !existing {
                return Ok(false);
            }
        }

        self.log_builder
            .action(MemLogEntryAction::DropGraph(object_id))?;

        Ok(true)
    }

    /// Returns the log array.
    pub fn into_log_entry(self) -> Result<Option<MemLogEntry>, StorageError> {
        let version_number = self.version_number.increment();
        Ok(self.log_builder.build(version_number)?)
    }
}
