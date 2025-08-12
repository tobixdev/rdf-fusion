use crate::memory::object_id::{EncodedObjectId, DEFAULT_GRAPH_ID};
use crate::memory::storage::index::{
    IndexConfiguration, IndexedQuad, ObjectIdScanPredicate,
};
use datafusion::arrow::array::{UInt32Array, UInt32Builder};
use rdf_fusion_encoding::object_id::ObjectIdArray;
use rdf_fusion_encoding::TermEncoding;
use std::collections::HashMap;
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
struct BufferedResults {
    results: Vec<BufferedResult>,
}

impl BufferedResults {
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
    pub fn scan_this_level(&self) -> Vec<EncodedObjectId> {
        self.0.keys().copied().collect()
    }

    pub fn contains_this_level(&self, object_id: EncodedObjectId) -> bool {
        self.0.contains_key(&object_id)
    }

    pub fn insert_this_level(&mut self, object_id: EncodedObjectId) -> bool {
        if self.0.contains_key(&object_id) {
            return false;
        }

        self.0.insert(object_id, TInner::default());
        true
    }

    pub fn remove_this_level(&mut self, object_id: EncodedObjectId) -> bool {
        self.0.remove(&object_id).is_some()
    }

    pub fn clear_entry(&mut self, object_id: EncodedObjectId) -> bool {
        if !self.0.contains_key(&object_id) {
            return false;
        }

        self.0.insert(object_id, TInner::default());
        true
    }

    pub fn clear_level(&mut self) {
        let keys = self.0.keys().copied().collect::<Vec<_>>();
        for value in keys {
            self.0.insert(value, TInner::default());
        }
    }

    fn traverse_impl(
        &self,
        configuration: &IndexConfiguration,
        mut traversal: IndexTraversal<TInner::ScanState>,
    ) -> IndexLevelActionResult<IndexLevelScanState<TInner::ScanState>> {
        let inner_results =
            traversal.try_collect_enough_results_for_batch(&self, configuration);

        // If no inner results can be found, the iterator is exhausted.
        if inner_results.results.is_empty() {
            return IndexLevelActionResult::empty_finished();
        }

        // If the buffered results are bigger than the batch size, buffer the last result for the
        // next call.
        let (inner_results, new_buffered) =
            Self::pop_last_if_bigger_than_batch_size(configuration, inner_results);
        traversal.buffered = new_buffered;

        // Build the result.
        let results = Self::coalesce_arrays(configuration, &inner_results);

        // If the end is reached, return the result and finish this subtree. Note that buffered
        // results are not consumed. Therefore, this check is enough.
        let num_results = inner_results.num_results();
        if traversal.consumed == self.0.len() {
            return IndexLevelActionResult::finished(num_results, Some(results));
        }

        IndexLevelActionResult {
            num_results,
            result: Some(results),
            new_state: Some(IndexLevelScanState::Traverse(traversal)),
        }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        mut traversal: IndexTraversal<TInner::ScanState>,
    ) -> IndexLevelActionResult<IndexLevelScanState<TInner::ScanState>> {
        let inner_results =
            traversal.try_collect_enough_results_for_batch(&self, configuration);

        // If no inner results can be found, the iterator is exhausted.
        if inner_results.results.is_empty() {
            return IndexLevelActionResult::empty_finished();
        }

        // If the buffered results are bigger than the batch size, buffer the last result for the
        // next call.
        let (inner_results, new_buffered) =
            Self::pop_last_if_bigger_than_batch_size(configuration, inner_results);
        traversal.buffered = new_buffered;

        // Build the result.
        let array = Self::build_object_ids_for_level(configuration, &inner_results);
        let results = Self::coalesce_arrays(configuration, &inner_results);
        let mut result_array = vec![array];
        result_array.extend(results);

        // If the end is reached, return the result and finish this subtree. Note that buffered
        // results are not consumed. Therefore, this check is enough.
        let num_results = inner_results.num_results();
        if traversal.consumed == self.0.len() {
            return IndexLevelActionResult::finished(num_results, Some(result_array));
        }

        IndexLevelActionResult {
            num_results,
            result: Some(result_array),
            new_state: Some(IndexLevelScanState::Scan(traversal)),
        }
    }

    fn pop_last_if_bigger_than_batch_size(
        configuration: &IndexConfiguration,
        mut buffered: BufferedResults,
    ) -> (BufferedResults, Option<BufferedResult>) {
        if buffered.num_results() > configuration.batch_size {
            let last_result = buffered.results.pop();
            (buffered, last_result)
        } else {
            (buffered, None)
        }
    }

    fn build_object_ids_for_level(
        configuration: &IndexConfiguration,
        inner_results: &BufferedResults,
    ) -> ObjectIdArray {
        let mut array_builder = UInt32Builder::new();
        for result in &inner_results.results {
            if result.object_id == DEFAULT_GRAPH_ID.0 {
                array_builder.append_nulls(result.num_results);
            } else {
                array_builder
                    .append_value_n(result.object_id.as_u32(), result.num_results);
            }
        }
        configuration
            .object_id_encoding
            .try_new_array(Arc::new(array_builder.finish()))
            .expect("TODO")
    }

    fn coalesce_arrays(
        configuration: &IndexConfiguration,
        inner_results: &BufferedResults,
    ) -> Vec<ObjectIdArray> {
        if inner_results.results.is_empty() {
            return Vec::new();
        }

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

/// The state of the iterator traversal. As the traversal is the same for both scan-types, they
/// share the same struct.
#[derive(Debug, Clone)]
struct IndexTraversal<TInnerState: Clone> {
    /// The predicate to scan for.
    predicate: Option<ObjectIdScanPredicate>,
    /// The inner states to set when moving to the next entry.
    default_state: TInnerState,
    /// Tracks how many entries have been consumed and should be skipped the next time.
    consumed: usize,
    /// The inner state.
    inner: TInnerState,
    /// The buffered result from the last iteration.
    buffered: Option<BufferedResult>,
}

impl<TInnerState: Clone> IndexTraversal<TInnerState> {
    pub fn try_collect_enough_results_for_batch<TLevel>(
        &mut self,
        index: &IndexLevel<TLevel>,
        configuration: &IndexConfiguration,
    ) -> BufferedResults
    where
        TLevel: IndexLevelImpl<ScanState = TInnerState>,
    {
        let mut results = Vec::new();

        // Collect the buffered result. If the result is already large enough, return it.
        if let Some(buffered) = self.buffered.take() {
            self.consumed += 1;
            if buffered.num_results == configuration.batch_size {
                return BufferedResults {
                    results: vec![buffered],
                };
            } else {
                results.push(buffered);
            }
        }

        let mut new_consumed = 0;
        let mut current_state = self.inner.clone();

        for (oid, next_level) in index.0.iter().skip(self.consumed) {
            if let Some(predicate) = &self.predicate
                && !predicate.evaluate(*oid)
            {
                new_consumed += 1;
                continue;
            }

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
            current_state = self.default_state.clone();
            if new_consumed >= configuration.batch_size {
                break;
            }
        }

        self.consumed += new_consumed;
        self.inner = current_state;
        BufferedResults { results }
    }
}

/// Represents the state of an action to execute.
#[derive(Debug, Clone)]
pub enum IndexLevelScanState<TInnerState: Clone> {
    /// For this level, the iterator should traverse the entries and collect the results from the
    /// inner levels.
    Traverse(IndexTraversal<TInnerState>),
    /// Same as [Self::Traverse] but also collects the elements from this level and returns them.
    Scan(IndexTraversal<TInnerState>),
}

impl<TInnerState: Clone> IndexLevelScanState<TInnerState> {
    pub fn traverse(
        predicate: Option<ObjectIdScanPredicate>,
        default_state: TInnerState,
    ) -> Self {
        let traversal = IndexTraversal {
            predicate,
            inner: default_state.clone(),
            default_state,
            consumed: 0,
            buffered: None,
        };
        Self::Traverse(traversal)
    }

    pub fn scan(
        predicate: Option<ObjectIdScanPredicate>,
        default_state: TInnerState,
    ) -> Self {
        let traversal = IndexTraversal {
            predicate,
            inner: default_state.clone(),
            default_state,
            consumed: 0,
            buffered: None,
        };
        Self::Scan(traversal)
    }
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
pub trait IndexLevelImpl: Default {
    /// The type of the traversal state.
    type ScanState: Clone;

    /// Inserts the triple into the index.
    fn insert(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> bool;

    /// Deletes the triple from the index.
    fn remove(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> bool;

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

    fn insert(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> bool {
        let part = triple.0[cur_depth];
        let content = self.0.entry(part).or_insert_with(|| TContent::default());
        content.insert(configuration, triple, cur_depth + 1)
    }

    fn remove(
        &mut self,
        configuration: &IndexConfiguration,
        quad: &IndexedQuad,
        cur_depth: usize,
    ) -> bool {
        let part = quad.0[cur_depth];

        let content = self.0.get_mut(&part);
        if let Some(content) = content {
            let inner_result = content.remove(configuration, quad, cur_depth + 1);
            if content.num_triples() == 0 {
                self.0.remove(&part);
            }
            inner_result
        } else {
            false
        }
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
            IndexLevelScanState::Traverse(traversal) => {
                self.traverse_impl(configuration, traversal)
            }
            IndexLevelScanState::Scan(traversal) => {
                self.scan_impl(configuration, traversal)
            }
        }
    }
}
