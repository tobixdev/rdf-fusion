use crate::memory::storage::index::quad_index_data::IndexData;
use crate::memory::storage::index::scan::MemQuadIndexScanIterator;
use crate::memory::storage::index::{
    DirectIndexRef, IndexConfiguration, IndexScanInstructions, IndexedQuad,
};
use std::collections::BTreeSet;

/// TODO
#[derive(Debug)]
pub struct MemQuadIndex {
    /// The index content.
    data: IndexData,
    /// The configuration of the index.
    configuration: IndexConfiguration,
}

impl MemQuadIndex {
    /// Creates a new [MemQuadIndex].
    pub fn new(configuration: IndexConfiguration) -> Self {
        let nullable_position = configuration
            .components
            .inner()
            .iter()
            .position(|c| c.gspo_index() == 0)
            .expect("There has to be a graph name");
        Self {
            data: IndexData::new(configuration.batch_size, nullable_position),
            configuration,
        }
    }

    /// Returns a reference to the content of the index.
    pub(super) fn data(&self) -> &IndexData {
        &self.data
    }

    /// Returns a reference to the index configuration.
    pub fn configuration(&self) -> &IndexConfiguration {
        &self.configuration
    }

    /// Returns the total number of quads.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Inserts a list of quads.
    ///
    /// Quads that already exist in the index are ignored.
    pub fn insert(&mut self, quads: impl IntoIterator<Item = IndexedQuad>) -> usize {
        let mut to_insert = BTreeSet::new();

        for quad in quads {
            to_insert.insert(quad);
        }

        self.data.insert(&to_insert)
    }

    /// TODO
    pub fn clear(&mut self) {
        self.data =
            IndexData::new(self.configuration.batch_size, self.data.nullable_position());
    }

    /// Removes a list of quads.
    ///
    /// Quads that do not exist in the index are ignored.
    pub fn remove(&mut self, quads: impl IntoIterator<Item = IndexedQuad>) -> usize {
        let mut to_insert = BTreeSet::new();

        for quad in quads {
            to_insert.insert(quad);
        }

        self.data.remove(&to_insert)
    }

    /// TODO
    pub fn scan_quads(
        &self,
        instructions: IndexScanInstructions,
    ) -> MemQuadIndexScanIterator<DirectIndexRef<'_>> {
        MemQuadIndexScanIterator::new(self, instructions)
    }
}
