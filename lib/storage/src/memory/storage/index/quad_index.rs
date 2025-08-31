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

    /// Returns a reference to the index configuration.
    pub fn configuration(&self) -> &IndexConfiguration {
        &self.configuration
    }

    /// Returns the total number of quads.
    pub fn len(&self) -> usize {
        self.content[3].len()
    }

    /// Returns an iterator that scans the index.
    pub fn scan_quads(
        &self,
        lookup: IndexScanInstructions,
    ) -> MemQuadIndexScanIterator<'_> {
        todo!()
    }

    /// Inserts a list of quads.
    ///
    /// Quads that already exist in the index are ignored.
    pub fn insert(&mut self, quads: impl IntoIterator<Item = IndexedQuad>) -> usize {
        let mut to_insert = [const { Vec::new() }; 4];

        for quad in quads {
            let Some(idx) = self.find_quad_insertion_index(&quad) else {
                continue;
            };

            for i in 0..4 {
                to_insert[i].push((idx, quad.0[i]));
            }
        }

        todo!("insert");

        to_insert[0].len()
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
        let mut to_delete = Vec::new();

        for quad in quads {
            if let Some(index) = self.find_quad(&quad) {
                to_delete.push(index)
            }
        }

        to_delete.sort_unstable();

        todo!();

        to_delete.len()
    }

    /// Tries to find `object_id` in the index and returns its index.
    fn find_quad(&self, quad: &IndexedQuad) -> Option<usize> {
        let range = self.content[0].find(quad.0[0], None)?;
        let range = self.content[1].find(quad.0[1], Some(range))?;
        let range = self.content[2].find(quad.0[2], Some(range))?;
        let range = self.content[3].find(quad.0[3], Some(range))?;

        assert_eq!(
            range.0.start, range.0.end,
            "The quad is inserted multiple times"
        );
        Some(range.0.start)
    }

    /// Tries to find the insertion index for the `quad` in the index.
    ///
    /// Returns [None] if the quad is already in the index
    fn find_quad_insertion_index(&self, quad: &IndexedQuad) -> Option<usize> {
        todo!()
    }
}
