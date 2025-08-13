use crate::memory::storage::index::hash_index::IndexContent;
use crate::memory::storage::index::level::IndexLevelImpl;
use crate::memory::storage::index::level_data::IndexDataScanState;
use crate::memory::storage::index::level_mapping::IndexLevelScanState;
use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstruction, IndexScanInstructions,
};
use datafusion::arrow::array::Array;
use rdf_fusion_encoding::object_id::ObjectIdArray;
use rdf_fusion_encoding::EncodingArray;
use std::collections::HashMap;
use tokio::sync::OwnedRwLockReadGuard;

/// The full type of the index scan iterator.
type IndexScanState =
    IndexLevelScanState<IndexLevelScanState<IndexLevelScanState<IndexDataScanState>>>;

/// An iterator that traverses the index and returns results.
///
/// # Batch Size
///
/// The iterator *tries* to adhere to the given batch size. However, the iterator may return smaller
/// batches in an attempt to return a full batch in the next iteration. Given the example below,
/// the iterator will return batches in the following sizes: `[8192] [8192] [6000] [8192]`.
/// So while smaller batches are coalesced into a single larger batch, the iterator returns a batch
/// with 6000 elements to "re-align" with entire batch sizes.
///
/// Index Lookup: `A ?var1 ?var2`
///
/// Index            Array Sizes
/// A -> B -> Data: `[8192] [8192] [5000]`
/// A -> C -> Data: `[1000]`
/// A -> D -> Data: `[8192]`
///
/// # Patterns Without Variables
///
/// If no variable is given (and only term patterns) the iterator will return a single item with an
/// empty vector. If no triple matches the pattern, then `None` will be returned.
pub struct MemHashIndexIterator {
    /// The iterator holds a read lock on the entire index such that another transaction cannot
    /// delete data from the index during iteration.
    index: OwnedRwLockReadGuard<IndexContent>,
    /// Additional context necessary for the iteration.
    configuration: IndexConfiguration,
    /// The states of the individual levels.
    state: Option<IndexScanState>,
}

impl MemHashIndexIterator {
    /// Creates a new [MemHashIndexIterator].
    pub(super) fn new(
        index: OwnedRwLockReadGuard<IndexContent>,
        configuration: IndexConfiguration,
        lookup: IndexScanInstructions,
    ) -> Self {
        let state = build_state(lookup.0);
        Self {
            index,
            configuration,
            state: Some(state),
        }
    }
}

/// TODO
#[derive(Debug, Clone)]
pub struct IndexScanBatch {
    pub num_results: usize,
    pub columns: HashMap<String, ObjectIdArray>,
}

impl IndexScanBatch {
    /// TODO
    pub fn no_results() -> Self {
        Self {
            num_results: 0,
            columns: HashMap::new(),
        }
    }

    /// TODO
    pub fn single_empty_result() -> Self {
        Self {
            num_results: 1,
            columns: HashMap::new(),
        }
    }

    /// TODO
    pub fn new_with_column(name: String, array: ObjectIdArray) -> IndexScanBatch {
        Self {
            num_results: array.array().len(),
            columns: vec![(name, array)].into_iter().collect(),
        }
    }
}

impl Iterator for MemHashIndexIterator {
    type Item = IndexScanBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let state = self.state.take()?;

        let mut collector = ScanCollector::new(self.configuration.batch_size);
        let (results, new_state) =
            self.index
                .index
                .scan(&self.configuration, state, &mut collector);
        self.state = new_state;

        if results > 0 {
            Some(collector.into_scan_batch(results, &self.configuration))
        } else {
            None
        }
    }
}

fn build_state(patterns: [IndexScanInstruction; 4]) -> IndexScanState {
    let data = create_state_for_data(patterns[3].clone());
    let first_level = create_state_for_level(patterns[2].clone(), data);
    let second_level = create_state_for_level(patterns[1].clone(), first_level);
    create_state_for_level(patterns[0].clone(), second_level)
}

fn create_state_for_data(scan_instruction: IndexScanInstruction) -> IndexDataScanState {
    match scan_instruction {
        IndexScanInstruction::Traverse(predicate) => {
            IndexDataScanState::Traverse { predicate }
        }
        IndexScanInstruction::Scan(name, predicate) => IndexDataScanState::Scan {
            name,
            predicate,
            consumed: 0,
        },
    }
}

pub fn create_state_for_level<TInner: Clone>(
    scan_instruction: IndexScanInstruction,
    inner: TInner,
) -> IndexLevelScanState<TInner> {
    match scan_instruction {
        IndexScanInstruction::Traverse(predicate) => {
            IndexLevelScanState::traverse(predicate, inner)
        }
        IndexScanInstruction::Scan(name, predicate) => {
            IndexLevelScanState::scan(name, predicate, inner)
        }
    }
}
