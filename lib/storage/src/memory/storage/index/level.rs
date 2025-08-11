use crate::memory::encoding::EncodedObjectIdPattern;
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::IndexConfiguration;
use crate::memory::storage::index::error::IndexDeletionError;
use crate::memory::storage::index::data::IndexedQuad;
use datafusion::arrow::array::UInt32Array;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::ObjectIdArray;
use std::collections::HashMap;
use std::iter::repeat_n;
use std::sync::Arc;

/// An index level is a mapping from [EncodedObjectId]. By traversing multiple index levels, users
/// can access the data in the index.
#[derive(Debug)]
pub struct IndexLevel<TInner: IndexLevelImpl>(HashMap<EncodedObjectId, TInner>);

#[derive(Debug, Clone)]
pub struct BufferedResult {
    num_results: usize,
    object_id: EncodedObjectId,
    inner: Vec<ObjectIdArray>,
}

#[derive(Debug, Clone)]
struct BufferedResults<TInnerState> {
    new_consumed: usize,
    results: Vec<BufferedResult>,
    new_state: TInnerState,
}

impl<TInnerState> BufferedResults<TInnerState> {
    pub fn num_results(&self) -> usize {
        self.results.iter().map(|r| r.num_results).sum()
    }
}

impl<TInner: IndexLevelImpl> Default for IndexLevel<TInner> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<TInner: IndexLevelImpl> IndexLevel<TInner> {
    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        buffered: Option<BufferedResult>,
        default_state: TInner::ScanState,
        inner: TInner::ScanState,
        consumed: usize,
    ) -> IndexLevelActionResult<IndexLevelScanState<TInner::ScanState>> {
        let inner_results = self.try_collect_enough_results_for_batch(
            configuration,
            buffered,
            &default_state,
            inner,
            consumed,
        );

        // If no inner results can be found, the iterator is exhausted.
        if inner_results.results.is_empty() {
            return IndexLevelActionResult::empty_finished();
        }

        // If the buffered results are bigger than the batch size, buffer the last result for the
        // next call.
        let (inner_results, new_buffered) =
            Self::pop_last_if_bigger_than_batch_size(configuration, inner_results);

        // Build the result.
        let array = Self::build_object_ids_for_level(configuration, &inner_results);
        let results = Self::coalesce_arrays(configuration, &inner_results);
        let mut result_array = vec![array];
        result_array.extend(results);

        // If the end is reached, return the result and finish this subtree. Note that buffered
        // results are not consumed. Therefore, this check is enough.
        let num_results = inner_results.num_results();
        if inner_results.new_consumed == self.0.len() {
            return IndexLevelActionResult::finished(num_results, Some(result_array));
        }

        let new_state = Some(IndexLevelScanState::Scan {
            buffered: new_buffered,
            consumed: inner_results.new_consumed,
            inner: inner_results.new_state,
            default_state,
        });
        IndexLevelActionResult {
            num_results,
            result: Some(result_array),
            new_state,
        }
    }

    fn try_collect_enough_results_for_batch(
        &self,
        configuration: &IndexConfiguration,
        buffered: Option<BufferedResult>,
        default_state: &<TInner as IndexLevelImpl>::ScanState,
        inner: <TInner as IndexLevelImpl>::ScanState,
        consumed: usize,
    ) -> BufferedResults<<TInner as IndexLevelImpl>::ScanState> {
        let mut results = Vec::new();
        if let Some(buffered) = buffered {
            if buffered.num_results == configuration.batch_size {
                return BufferedResults {
                    new_consumed: consumed,
                    new_state: inner,
                    results: vec![buffered],
                };
            } else {
                results.push(buffered);
            }
        }

        let mut new_consumed = 0;
        let mut current_state = inner;

        for (oid, next_level) in self.0.iter().skip(consumed) {
            let inner_result = next_level.scan(configuration, current_state);

            // An inner result was found.
            if let Some(inner) = inner_result.result {
                results.push(BufferedResult {
                    object_id: *oid,
                    inner,
                    num_results: inner_result.num_results,
                });
            }

            // If the inner result was not consumed, the next iteration must start from here.
            if let Some(new_state) = inner_result.new_state {
                current_state = new_state;
                break;
            }
            new_consumed += 1;

            // If the batch size is reached, return the result.
            current_state = default_state.clone();
            if new_consumed >= configuration.batch_size {
                break;
            }
        }

        BufferedResults {
            new_consumed: consumed + new_consumed,
            new_state: current_state,
            results,
        }
    }

    fn pop_last_if_bigger_than_batch_size(
        configuration: &IndexConfiguration,
        mut buffered: BufferedResults<<TInner as IndexLevelImpl>::ScanState>,
    ) -> (
        BufferedResults<<TInner as IndexLevelImpl>::ScanState>,
        Option<BufferedResult>,
    ) {
        if buffered.num_results() > configuration.batch_size {
            let last_result = buffered.results.pop();
            (buffered, last_result)
        } else {
            (buffered, None)
        }
    }

    fn build_object_ids_for_level(
        configuration: &IndexConfiguration,
        inner_results: &BufferedResults<<TInner as IndexLevelImpl>::ScanState>,
    ) -> ObjectIdArray {
        let values = inner_results
            .results
            .iter()
            .flat_map(|r| repeat_n(r.object_id.as_u32(), r.num_results));
        let u32_array = UInt32Array::from_iter_values(values);
        configuration
            .object_id_encoding
            .try_new_array(Arc::new(u32_array))
            .expect("TODO")
    }

    fn coalesce_arrays(
        configuration: &IndexConfiguration,
        inner_results: &BufferedResults<<TInner as IndexLevelImpl>::ScanState>,
    ) -> Vec<ObjectIdArray> {
        let mut result = Vec::new();
        for i in 0..inner_results.results[0].inner.len() {
            let iterator = inner_results
                .results
                .iter()
                .flat_map(|r| r.inner[i].object_ids().values().iter())
                .copied();
            let u32_array = UInt32Array::from_iter_values(iterator);
            let array = configuration
                .object_id_encoding
                .try_new_array(Arc::new(u32_array))
                .expect("TODO");
            result.push(array)
        }
        result
    }
}

pub fn create_state_for_level<TInner: Clone>(
    pattern: EncodedObjectIdPattern,
    inner: TInner,
) -> IndexLevelScanState<TInner> {
    match pattern {
        EncodedObjectIdPattern::ObjectId(object_id) => {
            IndexLevelScanState::Lookup { object_id, inner }
        }
        EncodedObjectIdPattern::Variable => IndexLevelScanState::Scan {
            buffered: None,
            default_state: inner.clone(),
            consumed: 0,
            inner,
        },
    }
}

/// Represents the state of an action to execute.
#[derive(Debug, Clone)]
pub enum IndexLevelScanState<TInnerState> {
    /// For this level, the iterator should only look up the next level.
    Lookup {
        /// The object id to look up.
        object_id: EncodedObjectId,
        /// The inner state
        inner: TInnerState,
    },
    /// For this level, the iterator should scan the entries and bind it to the given variable.
    Scan {
        buffered: Option<BufferedResult>,
        /// The inner states to set when moving to the next entry.
        default_state: TInnerState,
        /// Tracks how many entries have been consumed and should be skipped the next time.
        consumed: usize,
        /// The inner state.
        inner: TInnerState,
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
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    );

    /// Deletes the triple from the index.
    fn delete_triple(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> Result<(), IndexDeletionError>;

    /// The number of entries in the index part.
    fn num_triples(&self) -> usize;

    /// Executes the action for scanning the index at `cur_depth`.
    fn scan(
        &self,
        configuration: &IndexConfiguration,
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
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) {
        let part = triple.0[cur_depth];
        let content = self
            .0
            .entry(part)
            .or_insert_with(|| TContent::create_empty());
        content.insert_triple(configuration, triple, cur_depth + 1);
    }

    fn delete_triple(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> Result<(), IndexDeletionError> {
        let part = triple.0[cur_depth];

        let content = self
            .0
            .get_mut(&part)
            .ok_or(IndexDeletionError::NonExistingTriple)?;
        content.delete_triple(configuration, triple, cur_depth + 1)?;

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
        configuration: &IndexConfiguration,
        state: Self::ScanState,
    ) -> IndexLevelActionResult<Self::ScanState> {
        match state {
            IndexLevelScanState::Lookup { object_id, inner } => {
                match self.0.get(&object_id) {
                    None => IndexLevelActionResult::empty_finished(),
                    Some(content) => {
                        let inner = content.scan(configuration, inner);
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
                buffered,
                default_state,
                consumed,
                inner,
            } => self.scan_impl(configuration, buffered, default_state, inner, consumed),
        }
    }
}
