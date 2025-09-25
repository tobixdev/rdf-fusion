use crate::memory::object_id::EncodedGraphObjectId;
use crate::memory::storage::index::quad_index_data::IndexData;
use crate::memory::storage::index::scan::MemQuadIndexScanIterator;
use crate::memory::storage::index::{
    DirectIndexRef, IndexConfiguration, IndexScanInstructions, IndexedQuad,
};
use std::collections::BTreeSet;

/// Represents a single permutation of a quad index held in-memory. The index is sorted from left
/// to right.
///
/// Given the [IndexConfiguration] GPOS, the index could look like this:
/// ```text
/// ?graph   ?predicate  ?object  ?subject
/// ┌─────┐    ┌─────┐   ┌─────┐   ┌─────┐
/// │   0 │    │   1 │   │   4 │   │   4 │
/// ├─────┤    ├─────┤   ├─────┤   ├─────┤
/// │   0 │    │   1 │   │   7 │   │   7 │
/// ├─────┤    ├─────┤   ├─────┤   ├─────┤
/// │   0 │    │   2 │   │   1 │   │   1 │
/// ├─────┤    ├─────┤   ├─────┤   ├─────┤
/// │ ... │    │ ... │   │ ... │   │ ... │
/// └─────┘    └─────┘   └─────┘   └─────┘
/// ```
///
/// The physical representation of the index in detaield in [IndexData].
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

    /// Clears the entire index
    pub fn clear(&mut self) {
        self.data =
            IndexData::new(self.configuration.batch_size, self.data.nullable_position());
    }

    /// Clears the given `graph_name`.
    pub fn clear_graph(&mut self, graph_name: EncodedGraphObjectId) {
        let index = self.data.nullable_position();
        self.data
            .clear_all_with_value_in_column(graph_name.0, index);
    }

    /// Creates a new iterator give the given scan `instructions`.
    pub fn scan_quads(
        &self,
        instructions: IndexScanInstructions,
    ) -> MemQuadIndexScanIterator<DirectIndexRef<'_>> {
        MemQuadIndexScanIterator::new(self, instructions)
    }
}
