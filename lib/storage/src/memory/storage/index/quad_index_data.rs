use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::{
    IndexScanInstructions, IndexedQuad, ObjectIdScanPredicate,
};
use datafusion::arrow::array::{Array, UInt32Array};
use itertools::Itertools;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Contains the data of the index.
///
/// TODO
#[derive(Debug)]
pub(super) struct IndexData {
    nullable_position: usize,
    row_group_size: usize,
    row_groups: Vec<MemRowGroup>,
}

impl IndexData {
    /// Creates a new [IndexColumn].
    pub fn new(batch_size: usize, nullable_position: usize) -> Self {
        Self {
            nullable_position,
            row_group_size: batch_size,
            row_groups: Vec::new(),
        }
    }

    /// Returns which column of this index is nullable
    pub fn nullable_position(&self) -> usize {
        self.nullable_position
    }

    /// Returns the number of elements in the index.
    pub fn len(&self) -> usize {
        self.row_groups
            .iter()
            .map(|row_group| row_group.len())
            .sum()
    }

    /// Finds the range of this index that these instructions could match.
    ///
    /// This handles two tasks.
    /// 1. Search all row groups that may contain quads that match the given instructions
    /// 2. If necessary, slice the first and last row group such that some filters must not be
    ///    evaluated during the actual scan.
    pub fn prune_relevant_row_groups(
        &self,
        instructions: &IndexScanInstructions,
    ) -> Vec<MemRowGroup> {
        let mut relevant_row_groups = self.row_groups.clone();

        for (column_idx, instruction) in instructions.0.iter().enumerate() {
            // If there is no filter (we only support In for now) we must abort and do the scan
            // over the current row group set.
            let Some(ObjectIdScanPredicate::In(set)) = instruction.predicate() else {
                break;
            };

            // If there are multiple values in the set, the pruning stops as these may result in a
            // non-contiguous range which is not supported for the pruning
            if set.len() != 1 {
                break;
            }
            let id = *set.iter().next().unwrap();

            // Find the first row group for which the given id is not before the first value of the
            // row group.
            let first_relevant = relevant_row_groups
                .iter()
                .enumerate()
                .filter_map(|(row_group_idx, row_group)| {
                    let column_chunk = &row_group.column_chunks[column_idx];
                    match column_chunk.find_range(id) {
                        FindRangeResult::After => None,
                        result => Some((row_group_idx, result)),
                    }
                })
                .next();

            // No batch contains any relevant data
            let Some((first_row_group, first_range_result)) = first_relevant else {
                return vec![];
            };

            // All other results (e.g., NotContained) indicate that the given id is not contained
            // in the first batch. As a result, it won't be contained in any other batch.
            let FindRangeResult::Contained(from, to) = first_range_result else {
                return vec![];
            };

            let mut new_relevant_row_groups =
                vec![relevant_row_groups[first_row_group].slice(from, to)];

            // Find the end of relevant row groups. Can be skipped if the end of the first check
            // was before the end of the row group.
            if to == relevant_row_groups[first_row_group].len() {
                for row_group in &relevant_row_groups[first_row_group + 1..] {
                    let column_chunk = &row_group.column_chunks[column_idx];
                    match column_chunk.find_range(id) {
                        FindRangeResult::Before => {
                            break;
                        }
                        FindRangeResult::Contained(from, to) => {
                            assert_eq!(
                                from, 0,
                                "From must be 0, otherwise early terminated"
                            );

                            // If the end of the range is before the end of the row group, slice and
                            // abort
                            if to < row_group.len() {
                                new_relevant_row_groups.push(row_group.slice(from, to));
                                break;
                            } else {
                                new_relevant_row_groups.push(row_group.clone());
                            }
                        }
                        FindRangeResult::After | FindRangeResult::NotContained(_) => {
                            unreachable!("Column is sorted")
                        }
                    }
                }
            }

            relevant_row_groups = new_relevant_row_groups;
        }

        relevant_row_groups
    }

    /// Insert `to_insert` into the index.
    pub fn insert(&mut self, to_insert: &BTreeSet<IndexedQuad>) -> usize {
        let mut count = 0;
        let mut row_group_idx = 0;
        let mut to_insert = to_insert.iter().peekable();

        while row_group_idx < self.row_groups.len() {
            let current_row_group = &mut self.row_groups[row_group_idx];

            let mut to_insert_row_group = BTreeSet::new();
            while let Some(current_quad) = to_insert.peek() {
                match current_row_group.find(current_quad) {
                    QuadFindResult::Before => {
                        to_insert_row_group.insert(to_insert.next().unwrap().clone());
                    }
                    QuadFindResult::Contained => {
                        // Skip to the next quad if already contained.
                        to_insert.next();
                    }
                    QuadFindResult::NotContained => {
                        to_insert_row_group.insert(to_insert.next().unwrap().clone());
                    }
                    QuadFindResult::After => {
                        // Stop collecting for this row group.
                        break;
                    }
                }
            }

            count += to_insert_row_group.len();
            current_row_group.insert(to_insert_row_group);

            row_group_idx += 1;
        }

        for chunk in to_insert.chunks(self.row_group_size).into_iter() {
            let chunk = chunk.collect::<Vec<_>>();
            let new_row_group = MemRowGroup::new(chunk);
            count += new_row_group.len();
            self.row_groups.push(new_row_group);
        }

        count
    }

    /// TODO
    pub fn remove(&mut self, to_remove: &BTreeSet<IndexedQuad>) -> usize {
        let mut count = 0;
        let mut row_group_idx = 0;
        let mut to_insert = to_remove.iter().peekable();

        while row_group_idx < self.row_groups.len() {
            let current_row_group = &mut self.row_groups[row_group_idx];

            let mut to_remove_row_group = BTreeSet::new();
            while let Some(current_quad) = to_insert.peek() {
                match current_row_group.find(current_quad) {
                    QuadFindResult::Before | QuadFindResult::NotContained => {
                        // Do nothing, the quad is not present.
                        to_insert.next();
                    }
                    QuadFindResult::Contained => {
                        to_remove_row_group.insert(to_insert.next().unwrap().clone());
                    }
                    QuadFindResult::After => {
                        // Stop collecting for this row group.
                        break;
                    }
                }
            }

            count += to_remove_row_group.len();
            current_row_group.remove(to_remove_row_group);

            if current_row_group.len() == 0 {
                self.row_groups.remove(row_group_idx);
            } else {
                row_group_idx += 1;
            }
        }

        // Remaining quads are not contained in any row group.

        count
    }
}

/// TODO
pub enum QuadFindResult {
    Before,
    Contained,
    NotContained,
    After,
}

/// In the following, we borrow terminology from [Apache Parquet](https://parquet.apache.org/), as
/// the data is organized similarly to their approach.
#[derive(Debug, Clone)]
pub(super) struct MemRowGroup {
    column_chunks: [MemColumnChunk; 4],
}

impl MemRowGroup {
    /// Creates a new [MemRowGroup] with the provided `quads`.
    ///
    /// Assumes that `quads` is sorted.
    pub fn new(quads: Vec<&IndexedQuad>) -> Self {
        let column_chunks: [MemColumnChunk; 4] = (0..4)
            .map(|idx| {
                quads
                    .iter()
                    .map(|quad| {
                        let v = quad.0[idx].as_u32();
                        (v != 0).then_some(v)
                    })
                    .collect::<Vec<_>>()
            })
            .map(MemColumnChunk::new)
            .collect::<Vec<_>>()
            .try_into()
            .expect("Should yield 4 columns");

        Self { column_chunks }
    }

    /// The length of this row group.
    pub fn len(&self) -> usize {
        self.column_chunks[3].len()
    }

    /// Inserts the given quads into this [MemRowGroup].
    ///
    /// This method may assume the following:
    /// - No quad is already contained in this row group
    pub fn insert(&mut self, mut quads: BTreeSet<IndexedQuad>) {
        let mut new_quads = self.quads();
        new_quads.append(&mut quads);

        let new_data = Self::new(new_quads.iter().collect());
        self.column_chunks = new_data.column_chunks;
    }

    /// Removes the given quads from this [MemRowGroup].
    ///
    /// This method may assume the following:
    /// - All quads are contained in this row group
    pub fn remove(&mut self, quads: BTreeSet<IndexedQuad>) {
        let new_quads = self.quads();
        let difference = new_quads.difference(&quads);
        let new_data = Self::new(difference.collect());
        self.column_chunks = new_data.column_chunks;
    }

    /// TODO
    pub fn find(&self, quads: &IndexedQuad) -> QuadFindResult {
        let mut from = 0;
        let mut to = self.len();

        for (chunk, id) in self.column_chunks.iter().zip(quads.0.iter()) {
            let chunk = chunk.slice(from, to);
            let (new_from, new_to) = match chunk.find_range(*id) {
                FindRangeResult::Before => {
                    return if from == 0 {
                        QuadFindResult::Before
                    } else {
                        QuadFindResult::NotContained
                    };
                }
                FindRangeResult::NotContained(_) => {
                    return QuadFindResult::NotContained;
                }
                FindRangeResult::Contained(from, to) => (from, to),
                FindRangeResult::After => {
                    return if to == self.len() {
                        QuadFindResult::After
                    } else {
                        QuadFindResult::NotContained
                    };
                }
            };

            from = new_from;
            to = new_to;
        }

        debug_assert_eq!(from, to - 1, "Could not identify a single quad."); // to is exclusive
        QuadFindResult::Contained
    }

    /// TODO
    fn quads(&self) -> BTreeSet<IndexedQuad> {
        let n = self.len();
        (0..n)
            .map(|i| {
                IndexedQuad([
                    self.column_chunks[0].data.value(i).into(),
                    self.column_chunks[1].data.value(i).into(),
                    self.column_chunks[2].data.value(i).into(),
                    self.column_chunks[3].data.value(i).into(),
                ])
            })
            .collect()
    }

    /// TODO
    fn slice(&self, from: usize, to: usize) -> MemRowGroup {
        MemRowGroup {
            column_chunks: [
                self.column_chunks[0].slice(from, to),
                self.column_chunks[1].slice(from, to),
                self.column_chunks[2].slice(from, to),
                self.column_chunks[3].slice(from, to),
            ],
        }
    }

    /// TODO
    pub fn into_arrays(self) -> [Arc<UInt32Array>; 4] {
        self.column_chunks.map(|c| c.data)
    }
}

/// TODO
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum FindRangeResult {
    Before,
    NotContained(usize),
    Contained(usize, usize),
    After,
}

#[derive(Debug, Clone)]
pub(super) struct MemColumnChunk {
    data: Arc<UInt32Array>,
}

impl MemColumnChunk {
    /// Creates a new [MemColumnChunk].
    pub fn new(data: Vec<Option<u32>>) -> Self {
        Self {
            data: Arc::new(UInt32Array::from(data)),
        }
    }

    /// The length of this column chunk.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// TODO
    pub fn find_range(&self, value: EncodedObjectId) -> FindRangeResult {
        let null_count = self.data.null_count();

        if value.as_u32() == 0 {
            if null_count == 0 {
                return FindRangeResult::Before;
            }
            return FindRangeResult::Contained(0, null_count);
        }

        // TODO: Check last value for early return

        let find_result = self.data.values().iter().position(|v| *v >= value.as_u32());

        let count_first_larger_value = match find_result {
            None => return FindRangeResult::After,
            Some(position) => {
                if position == 0 && self.data.values()[0] != value.as_u32() {
                    return FindRangeResult::Before;
                } else if self.data.values()[position] != value.as_u32() {
                    return FindRangeResult::NotContained(null_count + position);
                } else {
                    position
                }
            }
        };

        let equal_count = self.data.values()[count_first_larger_value..]
            .iter()
            .take_while(|v| **v == value.as_u32())
            .count();

        FindRangeResult::Contained(
            count_first_larger_value,
            count_first_larger_value + equal_count,
        )
    }

    /// TODO
    fn slice(&self, from: usize, to: usize) -> MemColumnChunk {
        debug_assert!(from < to, "From must be smaller than to");
        let len = to - from;
        Self {
            data: Arc::new(self.data.slice(from, len)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::index::IndexScanInstruction;
    use insta::assert_debug_snapshot;
    use std::collections::HashSet;

    #[test]
    fn test_memcolumnchunk_slice_simple() {
        let chunk = MemColumnChunk::new(vec![Some(10), Some(20), Some(30), Some(40)]);
        assert_eq!(chunk.slice(1, 3).data.values(), &[20, 30]);
    }

    #[test]
    fn test_memcolumnchunk_slice_of_slice() {
        let chunk = MemColumnChunk::new(vec![
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
        ]);
        let mid_slice = chunk.slice(1, 5); // [6, 7, 8, 9]
        assert_eq!(mid_slice.slice(1, 3).data.values(), &[7, 8]);
    }

    #[test]
    fn test_empty_indexdata() {
        let index = IndexData::new(2, 0);
        assert_eq!(index.len(), 0);
        assert_eq!(index.row_groups.len(), 0);
    }

    #[test]
    fn test_insert_and_len_single_row_group() {
        let mut index = IndexData::new(4, 0);
        let items = quad_set([1, 2, 3]);
        index.insert(&items);

        assert_eq!(index.len(), 3);
        assert_eq!(index.row_groups.len(), 1);
        assert_eq!(index.row_groups[0].len(), 3);
    }

    #[test]
    fn test_insert_and_len_multiple_row_groups() {
        let mut index = IndexData::new(2, 0);
        let items = quad_set([10, 20, 30, 40, 50]);
        index.insert(&items);

        assert_eq!(index.len(), 5);
        assert_eq!(index.row_groups.len(), 3);
        assert_eq!(index.row_groups[0].len(), 2);
        assert_eq!(index.row_groups[1].len(), 2);
        assert_eq!(index.row_groups[2].len(), 1);
    }

    #[test]
    fn test_insert_empty_set_no_effect() {
        let mut index = IndexData::new(2, 0);
        let items = quad_set([]);
        index.insert(&items);

        assert_eq!(index.len(), 0);
        assert_eq!(index.row_groups.len(), 0);
    }

    #[test]
    fn test_inserting_multiple_batches_and_content() {
        let mut index = IndexData::new(3, 0);
        let items = quad_set([11, 12, 13, 14, 15, 16]);
        index.insert(&items);

        assert_eq!(index.row_groups.len(), 2);
        assert_eq!(index.row_groups[0].len(), 3);
        assert_eq!(index.row_groups[1].len(), 3);
    }

    #[test]
    fn test_inserting_duplicate_quads() {
        let mut index = IndexData::new(3, 0);
        let mut items = quad_set([1, 2, 3]);
        index.insert(&items);
        assert_eq!(index.len(), 3);

        // Insert overlapping items again
        items = quad_set([2, 3, 4]);
        index.insert(&items);

        assert_eq!(index.len(), 4);
    }

    #[test]
    fn test_nullable_indexdata_insert() {
        let mut index = IndexData::new(2, 0);
        let items = quad_set([0, 1, 2]);
        index.insert(&items);
    }

    #[test]
    fn test_memrowgroup_insert_to_empty() {
        let quads: Vec<IndexedQuad> = [10, 20, 30].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(vec![]);

        group.insert(quads.into_iter().collect());
        let arrays = group.clone().into_arrays();
        let values = arrays[0].values();
        assert_eq!(values, &[10, 20, 30]);
    }

    #[test]
    fn test_memrowgroup_insert_appends_to_existing() {
        // Insert after initial values, nothing overlaps
        let initial: Vec<IndexedQuad> = [10, 20].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(initial.iter().collect());
        let new_quads: Vec<IndexedQuad> = [30, 40].into_iter().map(|i| quad(i)).collect();

        group.insert(new_quads.into_iter().collect());
        let arrays = group.clone().into_arrays();
        let values = arrays[0].values();
        assert_eq!(values, &[10, 20, 30, 40]);
    }

    #[test]
    fn test_memrowgroup_insert_inserts_in_middle() {
        // Insert in the middle
        let initial: Vec<IndexedQuad> = [10, 30].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(initial.iter().collect());
        let new_quads: Vec<IndexedQuad> = [20].into_iter().map(|i| quad(i)).collect();

        group.insert(new_quads.into_iter().collect());
        let arrays = group.clone().into_arrays();
        let values = arrays[0].values();
        assert_eq!(values, &[10, 20, 30]);
    }

    #[test]
    fn test_memrowgroup_insert_with_nulls() {
        let initial: Vec<IndexedQuad> = [0, 2].into_iter().map(|i| quad(i)).collect();
        let mut group = MemRowGroup::new(initial.iter().collect());

        let new_quads: Vec<IndexedQuad> = [1].into_iter().map(|i| quad(i)).collect();
        group.insert(new_quads.into_iter().collect());

        let arrays = group.clone().into_arrays();
        assert_eq!(arrays[0].values(), &[0, 1, 2]);
    }

    #[test]
    fn test_prune_empty_index() {
        let index = IndexData::new(2, 0);
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert!(relevant.is_empty());
    }

    #[test]
    fn test_prune_no_filter_returns_all_groups() {
        let mut index = IndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);
        assert_eq!(relevant.len(), index.row_groups.len());
    }

    #[test]
    fn test_prune_filter_single_quad_present() {
        let mut index = IndexData::new(2, 0);
        let items = quad_set([10, 20, 30, 40]);
        index.insert(&items);

        // Only filter first column, look for value 30 which should be in second row group
        let predicate =
            ObjectIdScanPredicate::In(HashSet::from([EncodedObjectId::from(30u32)]));
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(predicate)),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert_eq!(relevant.len(), 1);
        assert_debug_snapshot!(relevant[0], @r"
        MemRowGroup {
            column_chunks: [
                MemColumnChunk {
                    data: PrimitiveArray<UInt32>
                    [
                      30,
                    ],
                },
                MemColumnChunk {
                    data: PrimitiveArray<UInt32>
                    [
                      30,
                    ],
                },
                MemColumnChunk {
                    data: PrimitiveArray<UInt32>
                    [
                      30,
                    ],
                },
                MemColumnChunk {
                    data: PrimitiveArray<UInt32>
                    [
                      30,
                    ],
                },
            ],
        }
        ")
    }

    /// This test aims to test the following scenario:
    /// - 1st row group matches completely
    /// - 2nd row group matches partly
    /// - 3rd row group doesn't match
    ///
    /// It is important that the algorithm stops after the 2nd group (the partial match).
    #[test]
    fn test_prune_filter_partial_match_breaks_early() {
        let mut index = IndexData::new(5, 0);

        index.insert(
            &[
                // 1st row group
                quad_from_values(10, 10, 10, 10),
                quad_from_values(10, 10, 10, 11),
                quad_from_values(10, 10, 10, 12),
                quad_from_values(10, 10, 10, 13),
                quad_from_values(10, 10, 10, 14),
                // 2nd row group
                quad_from_values(10, 10, 10, 15),
                quad_from_values(10, 10, 10, 16),
                quad_from_values(10, 10, 10, 17),
                quad_from_values(10, 10, 10, 18),
                quad_from_values(20, 5, 5, 5),
                // 3rd row group
                quad_from_values(20, 5, 5, 6),
                quad_from_values(20, 5, 5, 7),
            ]
            .into_iter()
            .collect(),
        );

        let predicate =
            ObjectIdScanPredicate::In(HashSet::from([EncodedObjectId::from(10u32)]));
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert_eq!(relevant.len(), 2);
        assert_eq!(relevant[0].len(), 5);
        assert_eq!(relevant[1].len(), 4);
    }

    #[test]
    fn test_prune_filter_breaks_early_on_last_row() {
        let mut index = IndexData::new(5, 0);

        index.insert(
            &[
                // 1st row group
                quad_from_values(10, 10, 10, 10),
                quad_from_values(10, 10, 10, 11),
                quad_from_values(10, 10, 10, 12),
                quad_from_values(10, 10, 10, 13),
                quad_from_values(11, 10, 10, 14),
                // 2nd row group
                quad_from_values(11, 10, 10, 15),
            ]
            .into_iter()
            .collect(),
        );

        let predicate =
            ObjectIdScanPredicate::In(HashSet::from([EncodedObjectId::from(10u32)]));
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert_eq!(relevant.len(), 1);
        assert_eq!(relevant[0].len(), 4);
    }

    #[test]
    fn test_prune_filter_multi_row_groups() {
        let mut index = IndexData::new(5, 0);

        index.insert(
            &[
                // 1st row group
                quad_from_values(0, 10, 10, 10),
                quad_from_values(0, 11, 10, 11),
                quad_from_values(0, 11, 10, 12),
                quad_from_values(0, 11, 10, 13),
                quad_from_values(0, 11, 10, 14),
                // 2nd row group
                quad_from_values(0, 11, 10, 21),
                quad_from_values(0, 11, 10, 22),
                quad_from_values(0, 11, 10, 23),
                quad_from_values(0, 11, 10, 24),
                quad_from_values(0, 11, 10, 25),
                // 3rd row group
                quad_from_values(0, 11, 10, 31),
                quad_from_values(0, 11, 12, 32),
            ]
            .into_iter()
            .collect(),
        );

        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(ObjectIdScanPredicate::In(
                HashSet::from([EncodedObjectId::from(0)]),
            ))),
            IndexScanInstruction::Traverse(Some(ObjectIdScanPredicate::In(
                HashSet::from([EncodedObjectId::from(11)]),
            ))),
            IndexScanInstruction::Traverse(Some(ObjectIdScanPredicate::In(
                HashSet::from([EncodedObjectId::from(10)]),
            ))),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert_eq!(relevant.len(), 3);
        assert_eq!(relevant[0].len(), 4);
        assert_eq!(relevant[1].len(), 5);
        assert_eq!(relevant[2].len(), 1);
    }

    #[test]
    fn test_prune_multiple_filters_start_fixed() {
        let mut index = IndexData::new(5, 0);

        index.insert(
            &[
                quad_from_values(10, 10, 10, 10),
                quad_from_values(10, 10, 12, 11),
                quad_from_values(10, 11, 12, 12),
                quad_from_values(20, 20, 20, 20),
            ]
            .into_iter()
            .collect(),
        );

        let predicate =
            ObjectIdScanPredicate::In(HashSet::from([EncodedObjectId::from(10u32)]));
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert_eq!(relevant.len(), 1);
        assert_eq!(relevant[0].len(), 1);
    }

    #[test]
    fn test_prune_multiple_filters_end_fixed() {
        let mut index = IndexData::new(5, 0);

        index.insert(
            &[
                quad_from_values(10, 9, 9, 10),
                quad_from_values(10, 10, 9, 10),
                quad_from_values(10, 10, 10, 10),
                quad_from_values(20, 20, 20, 20),
            ]
            .into_iter()
            .collect(),
        );

        let predicate =
            ObjectIdScanPredicate::In(HashSet::from([EncodedObjectId::from(10u32)]));
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(Some(predicate.clone())),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert_eq!(relevant.len(), 1);
        assert_eq!(relevant[0].len(), 1);
    }

    #[test]
    fn test_prune_filter_single_quad_absent() {
        let mut index = IndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let predicate =
            ObjectIdScanPredicate::In(HashSet::from([EncodedObjectId::from(99u32)]));
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(predicate)),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert!(relevant.is_empty());
    }

    #[test]
    fn test_prune_relevant_row_groups_in_predicate_multiple_values_no_pruning() {
        let mut index = IndexData::new(2, 0);
        let items = quad_set([1, 2, 3, 4]);
        index.insert(&items);

        let set =
            HashSet::from([EncodedObjectId::from(2u32), EncodedObjectId::from(3u32)]);
        let predicate = ObjectIdScanPredicate::In(set);
        let instructions = IndexScanInstructions([
            IndexScanInstruction::Traverse(Some(predicate)),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let relevant = index.prune_relevant_row_groups(&instructions);

        assert_eq!(relevant.len(), index.row_groups.len());
    }

    #[test]
    fn test_find_range_all_nulls() {
        let chunk = MemColumnChunk::new(vec![None, None, None]);
        let value = EncodedObjectId::from(0u32);
        let result = chunk.find_range(value);
        assert_eq!(result, FindRangeResult::Contained(0, 3));
    }

    #[test]
    fn test_find_range_nulls_before_data() {
        let chunk = MemColumnChunk::new(vec![None, None, Some(3), Some(5), Some(7)]);
        let result_null = chunk.find_range(EncodedObjectId::from(0u32));
        assert_eq!(result_null, FindRangeResult::Contained(0, 2));

        let result_val = chunk.find_range(EncodedObjectId::from(5u32));
        assert_eq!(result_val, FindRangeResult::Contained(3, 4));
    }

    #[test]
    fn test_find_range_value_present_single() {
        let chunk = MemColumnChunk::new(vec![Some(2), Some(4), Some(6)]);
        let result = chunk.find_range(EncodedObjectId::from(4u32));
        assert_eq!(result, FindRangeResult::Contained(1, 2));
    }

    #[test]
    fn test_find_range_value_present_multiple() {
        let chunk = MemColumnChunk::new(vec![Some(4), Some(4), Some(4), Some(5)]);
        let result = chunk.find_range(EncodedObjectId::from(4u32));
        assert_eq!(result, FindRangeResult::Contained(0, 3));
    }

    #[test]
    fn test_find_range_value_not_present_between() {
        let chunk = MemColumnChunk::new(vec![Some(1), Some(3), Some(5), Some(7)]);
        let result = chunk.find_range(EncodedObjectId::from(4u32));
        // 4 is not present, but would fall between 3 (idx 1) and 5 (idx 2)
        assert_eq!(result, FindRangeResult::NotContained(2));
    }

    #[test]
    fn test_find_range_value_too_small_and_too_big() {
        let chunk = MemColumnChunk::new(vec![Some(10), Some(20), Some(30)]);
        // Value smaller than any element
        let result_before = chunk.find_range(EncodedObjectId::from(2u32));
        assert_eq!(result_before, FindRangeResult::Before);

        // Value greater than any element
        let result_after = chunk.find_range(EncodedObjectId::from(50u32));
        assert_eq!(result_after, FindRangeResult::After);
    }

    /// Creates a quad where all four terms have the same u32 value
    fn quad(val: u32) -> IndexedQuad {
        IndexedQuad([
            EncodedObjectId::from(val),
            EncodedObjectId::from(val),
            EncodedObjectId::from(val),
            EncodedObjectId::from(val),
        ])
    }

    /// Creates a quad where all four terms have the same u32 value
    fn quad_from_values(val1: u32, val2: u32, val3: u32, val4: u32) -> IndexedQuad {
        IndexedQuad([
            EncodedObjectId::from(val1),
            EncodedObjectId::from(val2),
            EncodedObjectId::from(val3),
            EncodedObjectId::from(val4),
        ])
    }

    /// Creates a quad set from a set of u32 values. Each element in the quad will have the same
    /// value.
    fn quad_set<I: IntoIterator<Item = u32>>(vals: I) -> BTreeSet<IndexedQuad> {
        vals.into_iter().map(quad).collect()
    }
}
