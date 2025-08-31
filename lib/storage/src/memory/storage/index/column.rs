use crate::memory::object_id::EncodedObjectId;
use datafusion::arrow::array::UInt32Array;
use std::fmt::Debug;
use std::iter::Peekable;
use std::slice::Iter;
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
    pub fn batches(&self) -> &[Arc<TIndexArray>] {
        &self.data
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
    ) -> Result<IndexRange, usize> {
        TIndexArray::find(&self.data, object_id, range)
    }

    /// Finds the insertion
    ///
    /// `range` can be used to restrict the search space.
    pub fn insert(&mut self, object_ids: &[(usize, EncodedObjectId)]) {
        TIndexArray::insert(&mut self.data, object_ids, self.batch_size);
    }
}

pub(super) trait IndexArray: Debug {
    /// Returns the number of elements in the column.
    fn len(arrays: &Vec<Arc<Self>>) -> usize;

    /// Tries to find the range of the given `object_id`.
    ///
    /// `range` can be used to restrict this search to a subset of the column.
    fn find(
        arrays: &Vec<Arc<Self>>,
        object_id: EncodedObjectId,
        range: Option<IndexRange>,
    ) -> Result<IndexRange, usize>;

    /// Inserts the values into the sorted values at the given indices. The indices are required to
    /// be in ascending order.
    ///
    /// If this causes a batch to become greater than `batch_size`, the batch is split into
    /// two.
    fn insert(
        arrays: &mut Vec<Arc<Self>>,
        insertions: &[(usize, EncodedObjectId)],
        batch_size: usize,
    );
}

/// Represents a range of indices in a column.
///
/// The end is inclusive.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(super) struct IndexRange(pub usize, pub usize);

impl IndexArray for UInt32Array {
    fn len(batches: &Vec<Arc<Self>>) -> usize {
        batches.iter().map(|a| a.len()).sum()
    }

    fn find(
        batches: &Vec<Arc<Self>>,
        object_id: EncodedObjectId,
        range: Option<IndexRange>,
    ) -> Result<IndexRange, usize> {
        /// Finds the first batch (and the start index) of the first occurrence of the given
        /// object id.
        fn find_start_for_object_id(
            batches: &[Arc<UInt32Array>],
            object_id: &EncodedObjectId,
            start: usize,
            end: usize,
        ) -> Result<(usize, usize, usize), usize> {
            let mut global_offset = 0;
            for (batch_idx, batch) in batches.iter().enumerate() {
                let is_start_beyond_batch = global_offset + batch.len() < start;
                let is_end_beyond_batch = global_offset + batch.len() < end;

                // Skip if we're not yet at the start
                if is_start_beyond_batch {
                    global_offset += batch.len();
                    continue;
                }

                // If the last element is still lower than the object id, skip to the next batch.
                let last_value = *batch
                    .values()
                    .last()
                    .expect("Index arrays are never empty.");
                if last_value < object_id.as_u32() && is_end_beyond_batch {
                    global_offset += batch.len();
                    continue;
                }

                // End if we're past the end (needed if value is not found)
                if end < global_offset {
                    unreachable!("End index is before start index");
                }

                let local_start = start.saturating_sub(global_offset);
                let local_end = (end + 1 - global_offset).min(batch.len());
                let slice = &batch.values()[local_start..local_end];

                let mut idx = slice
                    .binary_search(&object_id.as_u32())
                    .map_err(|idx| global_offset + local_start + idx)?;

                while idx > 0 && slice[idx - 1] == slice[idx] {
                    idx -= 1;
                }

                return Ok((batch_idx, global_offset, global_offset + local_start + idx));
            }

            // If the start has not been found, the end index should be the insertion.
            Err(end)
        }

        /// Finds the last occurrence of the given object id.
        fn find_end_for_object_id(
            batches: &[Arc<UInt32Array>],
            object_id: &EncodedObjectId,
            mut global_offset: usize,
            start: usize,
            end: usize,
        ) -> usize {
            for batch in batches.iter() {
                // End if we're past the end
                if end < global_offset {
                    break;
                }

                // If the last element is still the object id, skip to the next batch.
                let last_value = *batch
                    .values()
                    .last()
                    .expect("Index arrays are never empty.");
                if last_value == object_id.as_u32() && end < global_offset + batch.len() {
                    global_offset += batch.len();
                    continue;
                }

                let local_start = start.saturating_sub(global_offset);
                let local_end = end - global_offset;
                let slice = &batch.values()[local_start..local_end];
                if let Ok(mut idx) = slice.binary_search(&object_id.as_u32()) {
                    while idx < slice.len() - 1 && slice[idx + 1] == slice[idx] {
                        idx += 1;
                    }

                    return global_offset + local_start + idx;
                }

                global_offset += batch.len();
            }

            // If this result is returned, the last object id of start..end is the provided object
            // id.
            end
        }

        let (start, end) = match range {
            None => (0, IndexArray::len(batches)),
            Some(IndexRange(from, to)) => (from, to),
        };

        let (batch_offset, global_offset, start_index) =
            find_start_for_object_id(batches.as_slice(), &object_id, start, end)?;
        let end_index = find_end_for_object_id(
            &batches[batch_offset..],
            &object_id,
            global_offset,
            start_index,
            end,
        );

        Ok(IndexRange(start_index, end_index))
    }

    fn insert(
        batches: &mut Vec<Arc<Self>>,
        insertions: &[(usize, EncodedObjectId)],
        batch_size: usize,
    ) {
        /// Gathers all insertions for the given batch.
        fn gather_insertions_for_batch(
            global_offset: usize,
            ins_iter: &mut Peekable<Iter<(usize, EncodedObjectId)>>,
            batch: &Arc<UInt32Array>,
        ) -> Vec<(usize, EncodedObjectId)> {
            let mut batch_insertions = Vec::new();

            while let Some((index, _)) = ins_iter.peek()
                && *index <= global_offset + batch.len()
            {
                let (index, value) = ins_iter.next().unwrap();
                batch_insertions.push((*index - global_offset, *value));
            }

            batch_insertions
        }

        /// Inserts the given insertions into the given batch. May return multiple batches if the
        /// batch was split.
        fn insert_into_batch(
            batch: &UInt32Array,
            insertions: &[(usize, EncodedObjectId)],
            batch_size: usize,
        ) -> Vec<Arc<UInt32Array>> {
            // copy existing values into Vec<u32>
            let mut vec: Vec<u32> = batch.values().iter().copied().collect();

            // insert in ascending order of index
            for (rel_idx, obj) in insertions.iter().rev() {
                vec.insert(*rel_idx, obj.as_u32());
            }

            if vec.len() > batch_size {
                let mid = vec.len() / 2;
                let left = Arc::new(UInt32Array::from(vec[..mid].to_vec()));
                let right = Arc::new(UInt32Array::from(vec[mid..].to_vec()));
                vec![left, right]
            } else {
                vec![Arc::new(UInt32Array::from(vec))]
            }
        }

        if insertions.is_empty() {
            return;
        }

        let mut global_offset = 0;
        let mut ins_iter = insertions.iter().peekable();

        let mut i = 0;
        while i < batches.len() {
            let batch = batches.get(i).unwrap();
            let batch_insertions =
                gather_insertions_for_batch(global_offset, &mut ins_iter, batch);

            if batch_insertions.is_empty() {
                i += 1;
                continue;
            }

            let new_batches =
                insert_into_batch(batch, batch_insertions.as_slice(), batch_size);

            // Remove old batch and insert new batches. TODO: Investigate linked list
            batches.remove(i);
            for new_batch in new_batches {
                global_offset += new_batch.len();
                batches.insert(i, new_batch);
                i += 1;
            }
        }

        // Insert remaining insertions
        let mut batch = Vec::new();
        while let Some((index, value)) = ins_iter.next() {
            debug_assert_eq!(*index, global_offset, "Invalid insertion index");
            batch.push(value.as_u32());

            if batch.len() == batch_size {
                let batch_to_push = std::mem::take(&mut batch);
                batches.push(Arc::new(UInt32Array::from(batch_to_push)));
            }
        }

        if !batch.is_empty() {
            batches.push(Arc::new(UInt32Array::from(batch)));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::index::column::IndexColumn;
    use crate::memory::storage::index::column::IndexRange;
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

    #[test]
    fn find_empty_column_returns_none() {
        let col = column([]);
        assert_eq!(col.find(enc(1), None), Err(0));
    }

    #[test]
    fn find_single_batch_hit() {
        let col = column([vec![1, 2, 3]]);
        assert_eq!(col.find(enc(2), None), Ok(IndexRange(1, 1)));
    }

    #[test]
    fn find_multiple_batches_hit() {
        let col = column([vec![1, 2], vec![3, 4], vec![5, 6]]);
        assert_eq!(col.find(enc(5), None), Ok(IndexRange(4, 4)));
    }

    #[test]
    fn find_multiple_batches_miss() {
        let col = column([vec![1, 2], vec![4], vec![5, 6]]);
        assert_eq!(col.find(enc(3), None), Err(2));
    }

    #[test]
    fn find_multiple_batches_miss_beyond() {
        let col = column([vec![1, 2], vec![4], vec![5, 6]]);
        assert_eq!(col.find(enc(7), None), Err(5));
    }

    #[test]
    fn find_with_range_limits_hit() {
        let col = column([vec![1, 2], vec![3, 4], vec![5, 6]]);
        assert_eq!(
            col.find(enc(4), Some(IndexRange(2, 3))),
            Ok(IndexRange(3, 3))
        );
    }

    #[test]
    fn find_with_range_limits_miss_before() {
        let col = column([vec![1, 2], vec![3, 4], vec![5, 6]]);
        assert_eq!(col.find(enc(2), Some(IndexRange(2, 6))), Err(2));
    }

    #[test]
    fn find_with_range_limits_miss_after() {
        let col = column([vec![1, 2], vec![3, 4], vec![5, 6]]);
        assert_eq!(col.find(enc(7), Some(IndexRange(2, 6))), Err(6));
    }

    #[test]
    fn insert_with_empty_insertions() {
        let mut data = column([vec![1, 3, 5]]);
        data.insert(&[]);
        assert_eq!(data.data, column([vec![1, 3, 5]]).data);
    }

    #[test]
    fn insert_simple() {
        let mut data = column([vec![1, 3, 5]]);
        data.insert(&[(1, enc(2)), (2, enc(4))]);
        assert_eq!(data.data, column([vec![1, 2, 3, 4, 5]]).data);
    }

    #[test]
    fn insert_with_empty_column() {
        let mut data = column([]);
        data.insert(&[(0, enc(1)), (0, enc(2))]);
        assert_eq!(data.data, column([vec![1, 2]]).data);
    }

    #[test]
    fn insert_with_empty_column_and_multiple_batches() {
        let mut data = column_with_batch_size(2, []);
        data.insert(&[(0, enc(1)); 5]);
        assert_eq!(data.data, column([vec![1, 1], vec![1, 1], vec![1]]).data);
    }

    #[test]
    fn insert_and_split_batches_if_needed() {
        let mut data = column_with_batch_size(4, [vec![1, 2, 3, 4]]);
        data.insert(&[(0, enc(1)), (3, enc(4))]);
        assert_eq!(data.data, column([vec![1, 1, 2], vec![3, 4, 4]]).data);
    }

    fn column<'a>(
        partitions: impl IntoIterator<Item = Vec<u32>>,
    ) -> IndexColumn<UInt32Array> {
        column_with_batch_size(16, partitions)
    }

    fn column_with_batch_size<'a>(
        batch_size: usize,
        partitions: impl IntoIterator<Item = Vec<u32>>,
    ) -> IndexColumn<UInt32Array> {
        let arrays = partitions
            .into_iter()
            .map(|p| Arc::new(UInt32Array::from(p.to_vec())))
            .collect();
        IndexColumn {
            batch_size,
            data: arrays,
        }
    }

    fn enc(n: u32) -> EncodedObjectId {
        EncodedObjectId::from(n) // replace with correct constructor if needed
    }
}
