use crate::memory::storage::index::hash_index::IndexContent;
use crate::memory::storage::index::level::IndexLevelImpl;
use crate::memory::storage::index::level::ScanState;
use crate::memory::storage::index::level_data::IndexData;
use crate::memory::storage::index::level_mapping::IndexLevel;
use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{IndexConfiguration, IndexScanInstructions};
use datafusion::arrow::array::Array;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::object_id::ObjectIdArray;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

/// The full type of the index scan iterator.
type IndexType = IndexLevel<IndexLevel<IndexLevel<IndexData>>>;

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
    /// The variables that are used in the patterns. This may contain duplicate variables.
    variables: [Option<String>; 4],
    /// The iterator holds a read lock on the entire index such that another transaction cannot
    /// delete data from the index during iteration.
    #[allow(
        dead_code,
        reason = "This is needed to ensure that the index stays valid (and locked) during iteration."
    )]
    index: Arc<OwnedRwLockReadGuard<IndexContent>>,
    /// Additional context necessary for the iteration.
    configuration: IndexConfiguration,
    /// The states of the individual levels.
    state: Option<<IndexType as IndexLevelImpl>::ScanState<'static>>,
}

/// See [MemHashIndexIterator].
#[derive(Debug, Clone)]
pub struct PreparedIndexScan {
    /// See [MemHashIndexIterator].
    index: Arc<OwnedRwLockReadGuard<IndexContent>>,
    /// See [MemHashIndexIterator].
    configuration: IndexConfiguration,
    /// See [MemHashIndexIterator].
    instructions: IndexScanInstructions,
}

impl PreparedIndexScan {
    pub(super) fn new(
        index: Arc<OwnedRwLockReadGuard<IndexContent>>,
        configuration: IndexConfiguration,
        instructions: IndexScanInstructions,
    ) -> Self {
        Self {
            index,
            configuration,
            instructions,
        }
    }

    /// TODO
    pub fn configuration(&self) -> &IndexConfiguration {
        &self.configuration
    }

    /// TODO
    pub fn create_iterator(self) -> MemHashIndexIterator {
        let variables = self
            .instructions
            .0
            .clone()
            .map(|i| i.scan_variable().map(|v| v.to_owned()));

        let configuration = self.configuration.clone();
        let instructions = self.instructions.0.to_vec();
        let scan_state = self
            .index
            .as_ref()
            .index
            .create_scan_state(&configuration, &instructions);

        let scan_state_static: <IndexType as IndexLevelImpl>::ScanState<'static> = unsafe {
            // SAFETY: The ScanState<'_> borrows from the backing data in the index,
            // which is protected and owned by the Arc<OwnedRwLockReadGuard<IndexContent>>
            // held in the iterator. This Arc is cloned into the iterator struct and
            // will be kept alive as long as the iterator exists.
            std::mem::transmute::<
                <IndexType as IndexLevelImpl>::ScanState<'_>,
                <IndexType as IndexLevelImpl>::ScanState<'static>,
            >(scan_state)
        };

        MemHashIndexIterator {
            variables,
            index: self.index.clone(),
            configuration: self.configuration,
            state: Some(scan_state_static),
        }
    }
}

impl Display for PreparedIndexScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", self.configuration)
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
        let (results, new_state) = state.drive_scan(&self.configuration, &mut collector);
        self.state = new_state;

        if results > 0 {
            Some(collector.into_scan_batch(
                results,
                self.variables.clone(),
                &self.configuration,
            ))
        } else {
            None
        }
    }
}
