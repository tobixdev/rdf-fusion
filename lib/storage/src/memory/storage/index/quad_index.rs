use crate::memory::storage::index::column::IndexColumn;
use crate::memory::storage::index::scan::MemQuadIndexScanIterator;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstructions, IndexedQuad,
};
use datafusion::arrow::array::UInt32Array;

/// TODO
#[derive(Debug)]
pub struct MemQuadIndex {
    /// The index content.
    content: [IndexColumn<UInt32Array>; 4],
    /// The configuration of the index.
    configuration: IndexConfiguration,
}

impl MemQuadIndex {
    /// Creates a new [MemQuadIndex].
    pub fn new(configuration: IndexConfiguration) -> Self {
        Self {
            content: [
                IndexColumn::new(configuration.batch_size),
                IndexColumn::new(configuration.batch_size),
                IndexColumn::new(configuration.batch_size),
                IndexColumn::new(configuration.batch_size),
            ],
            configuration,
        }
    }

    /// Returns a reference to the content of the index.
    pub fn content(&self) -> &[IndexColumn<UInt32Array>; 4] {
        &self.content
    }

    /// Returns a reference to the index configuration.
    pub fn configuration(&self) -> &IndexConfiguration {
        &self.configuration
    }

    /// Returns the total number of quads.
    pub fn len(&self) -> usize {
        self.content[3].len()
    }

    /// Inserts a list of quads.
    ///
    /// Quads that already exist in the index are ignored.
    pub fn insert(&mut self, quads: impl IntoIterator<Item = IndexedQuad>) -> usize {
        let mut to_insert = Vec::new();

        for quad in quads {
            let Some(idx) = self.find_quad_insertion_index(&quad) else {
                continue;
            };

            to_insert.push((idx, quad))
        }
        to_insert.sort_unstable_by_key(|(_, quad)| quad.0[0]);

        for element in 0..4 {
            let to_insert_column = to_insert
                .iter()
                .map(|(idx, quad)| (*idx, quad.0[element]))
                .collect::<Vec<_>>();
            self.content[element].insert(&to_insert_column)
        }

        to_insert.len()
    }

    /// TODO
    pub fn clear(&mut self) -> () {
        self.content = [
            IndexColumn::new(self.configuration.batch_size),
            IndexColumn::new(self.configuration.batch_size),
            IndexColumn::new(self.configuration.batch_size),
            IndexColumn::new(self.configuration.batch_size),
        ];
    }

    /// Removes a list of quads.
    ///
    /// Quads that do not exist in the index are ignored.
    pub fn remove(&mut self, quads: impl IntoIterator<Item = IndexedQuad>) -> usize {
        let mut to_remove = Vec::new();

        for quad in quads {
            if let Some(index) = self.find_quad(&quad) {
                to_remove.push(index)
            }
        }

        to_remove.sort_unstable();

        todo!();

        to_remove.len()
    }

    /// Tries to find `object_id` in the index and returns its index.
    fn find_quad(&self, quad: &IndexedQuad) -> Option<usize> {
        let range = self.content[0].find(quad.0[0], None).ok()?;
        let range = self.content[1].find(quad.0[1], Some(range)).ok()?;
        let range = self.content[2].find(quad.0[2], Some(range)).ok()?;
        let range = self.content[3].find(quad.0[3], Some(range)).ok()?;

        assert_eq!(range.0, range.1, "The quad is inserted multiple times");
        Some(range.0)
    }

    /// Tries to find the insertion index for the `quad` in the index.
    ///
    /// Returns [None] if the quad is already in the index
    fn find_quad_insertion_index(&self, quad: &IndexedQuad) -> Option<usize> {
        let range = match self.content[0].find(quad.0[0], None) {
            Ok(range) => range,
            Err(insertion_index) => return Some(insertion_index),
        };

        let range = match self.content[1].find(quad.0[1], Some(range)) {
            Ok(range) => range,
            Err(insertion_index) => return Some(insertion_index),
        };

        let range = match self.content[2].find(quad.0[2], Some(range)) {
            Ok(range) => range,
            Err(insertion_index) => return Some(insertion_index),
        };

        match self.content[3].find(quad.0[3], Some(range)) {
            Ok(_) => None, // The quad is already in the index.
            Err(insertion_index) => Some(insertion_index),
        }
    }
}
