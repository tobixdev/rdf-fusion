use crate::memory::object_id::EncodedObjectId;
use datafusion::arrow::array::{Array, UInt32Array};
use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

/// Contains a column of the index.
///
/// Each column is divided into multiple sorted parts. The first column has a single part. Then,
/// for each value in the first column, the next column has a single sorted part. For every tuple
/// in the first two columns, the third column has a single sorted part. And so on.
///
/// Each column is divided into multiple batches. The column aims to fill each batch with
/// `batch_size` elements. However, this is not guaranteed, as keeping this property while allowing
/// insertions and deletions is costly.
#[derive(Debug)]
pub(super) struct IndexColumn<TIndexArray: IndexArray> {
    batch_size: usize,
    data: Vec<Arc<TIndexArray>>,
}

impl<TIndexArray: IndexArray> IndexColumn<TIndexArray> {
    /// Creates a new [IndexColumn].
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            data: Vec::new(),
        }
    }

    /// Returns the number of elements in the column.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Finds the range of indices that contain the given object id.
    ///
    /// `range` can be used to restrict the search space.
    pub fn find(
        &self,
        object_id: EncodedObjectId,
        range: Option<IndexRange>,
    ) -> Option<IndexRange> {
        TIndexArray::find(&self.data, object_id, range)
    }

    /// Finds the insertion
    ///
    /// `range` can be used to restrict the search space.
    pub fn insert(&mut self, object_ids: &[(usize, EncodedObjectId)]) {
        TIndexArray::insert(&mut self.data, object_ids)
    }

    /// Finds the range of indices that contain the given object id.
    ///
    /// `range` can be used to restrict the search space.
    pub fn find_relevant_batches(
        &self,
        object_id: EncodedObjectId,
        range: Option<BatchRange>,
    ) -> Option<BatchRange> {
        TIndexArray::find_relevant_batches(&self.data, object_id, range)
    }
}

trait IndexArray: Debug {
    /// Returns the number of elements in the column.
    fn len(arrays: &Vec<Arc<Self>>) -> usize;

    /// Tries to find the range of the given `object_id`.
    ///
    /// `range` can be used to restrict this search to a subset of the column.
    fn find(
        arrays: &Vec<Arc<Self>>,
        object_id: EncodedObjectId,
        range: Option<IndexRange>,
    ) -> Option<IndexRange>;

    /// Identifies the relevant batches for the given `object_id`. This operations does not scan
    /// the actual values but only looks at the first (= lowest) and last (= highest) element to
    /// determine whether the object id *could* be part of a batch.
    ///
    /// If a `range` is given, only these batches will be considered.
    fn find_relevant_batches(
        arrays: &Vec<Arc<Self>>,
        object_id: EncodedObjectId,
        range: Option<BatchRange>,
    ) -> Option<BatchRange>;

    /// Inserts the values into the sorted values at the given indices.
    ///
    /// If this causes a batch to become greater than [Self::batch_size], the batch is split into
    /// two.
    fn insert(arrays: &mut Vec<Arc<Self>>, insertions: &[(usize, EncodedObjectId)]);
}

/// Represents a range of batches in a column.
pub(super) struct BatchRange(pub Range<usize>);

/// Represents a range of indices in a column.
pub(super) struct IndexRange(pub Range<usize>);

impl IndexArray for UInt32Array {
    fn len(arrays: &Vec<Arc<Self>>) -> usize {
        arrays.iter().map(|a| a.len()).sum()
    }

    fn find(
        arrays: &Vec<Arc<Self>>,
        object_id: EncodedObjectId,
        range: Option<IndexRange>,
    ) -> Option<IndexRange> {
        todo!()
    }

    fn find_relevant_batches(
        arrays: &Vec<Arc<Self>>,
        object_id: EncodedObjectId,
        range: Option<BatchRange>,
    ) -> Option<BatchRange> {
        todo!()
    }

    fn insert(arrays: &mut Vec<Arc<Self>>, insertions: &[(usize, EncodedObjectId)]) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::storage::index::column::IndexColumn;
    use datafusion::arrow::array::UInt32Array;
    use std::sync::Arc;

    #[test]
    fn len_uint32_empty() {
        assert_eq!(column([]).len(), 0)
    }

    #[test]
    fn len_uint32() {
        assert_eq!(column([vec![1], vec![2]]).len(), 2)
    }

    fn column<'a>(
        partitions: impl IntoIterator<Item = Vec<u32>>,
    ) -> IndexColumn<UInt32Array> {
        let arrays = partitions
            .into_iter()
            .map(|p| Arc::new(UInt32Array::from(p.to_vec())))
            .collect();
        IndexColumn {
            batch_size: 16,
            data: arrays,
        }
    }
}
