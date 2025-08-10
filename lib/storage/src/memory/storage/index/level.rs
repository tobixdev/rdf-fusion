use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::error::IndexDeletionError;
use crate::memory::storage::index::index::IndexedTriple;
use datafusion::arrow::array::{Array, UInt32Array};
use rdf_fusion_encoding::object_id::{ObjectIdArray, ObjectIdEncoding};
use rdf_fusion_encoding::TermEncoding;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

/// An index level is a mapping from [EncodedObjectId]. By traversing multiple index levels, users
/// can access the data in the index.
#[derive(Debug)]
pub struct IndexLevel<TInner: IndexLevelImpl>(HashMap<EncodedObjectId, TInner>);

impl<TInner: IndexLevelImpl> Default for IndexLevel<TInner> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<TInner: IndexLevelImpl> IndexLevel<TInner> {
    fn scan_impl(
        &self,
        context: &IndexIterationContext,
        default_state: TInner::ScanState,
        inner: TInner::ScanState,
        consumed: usize,
    ) -> IndexLevelActionResult<IndexLevelScanState<TInner::ScanState>> {
        let mut cur_index = consumed;
        let mut cur_state = inner;

        let (oid, num_results, mut result_array, new_state) = loop {
            let (oid, next_level) = self
                .0
                .iter()
                .skip(cur_index)
                .next()
                .expect("Otherwise this state would not have been created.");
            let inner_result = next_level.scan(context, cur_state);

            // An inner result was found.
            if let Some(result_array) = inner_result.result {
                break (
                    oid.clone(),
                    inner_result.num_results,
                    result_array,
                    inner_result.new_state,
                );
            }

            // Nothing matches anymore in this subtree.
            if self.0.len() == cur_index {
                return IndexLevelActionResult::empty_finished();
            }

            // Else try the next subtree.
            cur_index += 1;
            cur_state = default_state.clone();
        };

        let values = iter::repeat_n(oid.as_u32(), num_results);
        let u32_array = UInt32Array::from_iter_values(values);
        let array = context
            .object_id_encoding
            .try_new_array(Arc::new(u32_array))
            .expect("TODO");
        result_array.insert(0, array);

        match new_state {
            None => {
                // If the end is reached, return the result and finish this subtree.
                if cur_index == self.0.len() - 1 {
                    return IndexLevelActionResult::finished(
                        num_results,
                        Some(result_array),
                    );
                }

                // If the end is not reached, prepare the next iteration and return the result.
                IndexLevelActionResult {
                    num_results,
                    result: Some(result_array),
                    new_state: Some(IndexLevelScanState::Scan {
                        consumed: cur_index + 1,
                        inner: default_state.clone(),
                        default_state,
                    }),
                }
            }
            Some(state) => IndexLevelActionResult {
                num_results,
                result: Some(result_array),
                new_state: Some(IndexLevelScanState::Scan {
                    default_state,
                    consumed: cur_index,
                    inner: state,
                }),
            },
        }
    }
}

/// Holds the entire state of an index iterator.
pub struct IndexIterationContext {
    /// The object id encoding.
    pub object_id_encoding: ObjectIdEncoding,
    /// The desired batch size. This iterator only provides a best-effort service for adhering to
    /// the batch size.
    pub batch_size: usize,
}

/// Represents the state of an action to execute.
#[derive(Debug, Clone)]
pub enum IndexLevelScanState<TInner> {
    /// For this level, the iterator should only look up the next level.
    Lookup {
        /// The object id to look up.
        object_id: EncodedObjectId,
        /// The inner state
        inner: TInner,
    },
    /// For this level, the iterator should scan the entries and bind it to the given variable.
    Scan {
        /// The inner states to set when moving to the next entry.
        default_state: TInner,
        /// Tracks how many entries have been consumed and should be skipped the next time.
        consumed: usize,
        /// The inner state.
        inner: TInner,
    },
}

/// The result of an action on an index level.
#[derive(Debug)]
pub struct IndexLevelActionResult<TState> {
    /// The number of results that have been returned.
    pub num_results: usize,
    /// The result of this level.
    pub result: Option<Vec<ObjectIdArray>>,
    /// The new state of the iterator at this level.
    pub new_state: Option<TState>,
}

impl<TState> IndexLevelActionResult<TState> {
    pub fn finished(num_results: usize, result: Option<Vec<ObjectIdArray>>) -> Self {
        Self {
            num_results,
            result,
            new_state: None,
        }
    }

    pub fn empty_finished() -> Self {
        Self {
            num_results: 0,
            result: None,
            new_state: None,
        }
    }
}

/// Contains the logic for a single index level.
pub trait IndexLevelImpl {
    /// The type of the traversal state.
    type ScanState: Clone;

    /// Creates a new empty index level.
    fn create_empty() -> Self;

    /// Inserts the triple into the index.
    fn insert_triple(
        &mut self,
        triple: &IndexedTriple,
        cur_depth: usize,
        batch_size: usize,
    );

    /// Deletes the triple from the index.
    fn delete_triple(
        &mut self,
        triple: &IndexedTriple,
        cur_depth: usize,
    ) -> Result<(), IndexDeletionError>;

    /// The number of entries in the index part.
    fn num_triples(&self) -> usize;

    /// Executes the action for scanning the index at `cur_depth`.
    fn scan(
        &self,
        context: &IndexIterationContext,
        state: Self::ScanState,
    ) -> IndexLevelActionResult<Self::ScanState>;
}

impl<TContent: IndexLevelImpl> IndexLevelImpl for IndexLevel<TContent> {
    type ScanState = IndexLevelScanState<TContent::ScanState>;

    fn create_empty() -> Self {
        Self::default()
    }

    fn insert_triple(
        &mut self,
        triple: &IndexedTriple,
        cur_depth: usize,
        target_size: usize,
    ) {
        let part = triple.0[cur_depth];

        let content = self
            .0
            .entry(part)
            .or_insert_with(|| TContent::create_empty());
        content.insert_triple(triple, cur_depth + 1, target_size);
    }

    fn delete_triple(
        &mut self,
        triple: &IndexedTriple,
        cur_depth: usize,
    ) -> Result<(), IndexDeletionError> {
        let part = triple.0[cur_depth];

        let content = self
            .0
            .get_mut(&part)
            .ok_or(IndexDeletionError::NonExistingTriple)?;
        content.delete_triple(triple, cur_depth + 1)?;

        if content.num_triples() == 0 {
            self.0.remove(&part);
        }

        Ok(())
    }

    fn num_triples(&self) -> usize {
        self.0.values().map(|c| c.num_triples()).sum()
    }

    fn scan(
        &self,
        context: &IndexIterationContext,
        state: Self::ScanState,
    ) -> IndexLevelActionResult<Self::ScanState> {
        match state {
            IndexLevelScanState::Lookup { object_id, inner } => {
                match self.0.get(&object_id) {
                    None => IndexLevelActionResult::empty_finished(),
                    Some(content) => {
                        let inner = content.scan(context, inner);
                        IndexLevelActionResult {
                            num_results: inner.num_results,
                            result: inner.result,
                            new_state: inner.new_state.map(|state| {
                                IndexLevelScanState::Lookup {
                                    object_id,
                                    inner: state,
                                }
                            }),
                        }
                    }
                }
            }
            IndexLevelScanState::Scan {
                default_state,
                consumed,
                inner,
            } => self.scan_impl(context, default_state, inner, consumed),
        }
    }
}
