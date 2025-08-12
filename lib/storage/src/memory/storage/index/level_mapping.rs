use crate::memory::object_id::{EncodedObjectId, DEFAULT_GRAPH_ID};
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanBatch, IndexedQuad, ObjectIdScanPredicate,
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
pub struct BufferedIndexScanBatch {
    object_id: EncodedObjectId,
    inner: IndexScanBatch,
}

#[derive(Debug, Clone)]
struct BufferedResults {
    results: Vec<BufferedIndexScanBatch>,
}

impl BufferedResults {
    pub fn num_results(&self) -> usize {
        self.results.iter().map(|r| r.inner.num_results).sum()
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

        if inner_results.results.is_empty() {
            return IndexLevelActionResult::finished(IndexScanBatch::no_results());
        }

        let (inner_results, new_buffered) =
            Self::pop_last_if_bigger_than_batch_size(configuration, inner_results);
        traversal.buffered = new_buffered;

        let batch = Self::coalesce_batches(configuration, &inner_results);
        if traversal.consumed == self.0.len() {
            return IndexLevelActionResult::finished(batch);
        }
        IndexLevelActionResult {
            batch,
            new_state: Some(IndexLevelScanState::Traverse(traversal)),
        }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        name: String,
        mut traversal: IndexTraversal<TInner::ScanState>,
    ) -> IndexLevelActionResult<IndexLevelScanState<TInner::ScanState>> {
        let inner_results =
            traversal.try_collect_enough_results_for_batch(&self, configuration);

        if inner_results.results.is_empty() {
            return IndexLevelActionResult::finished(IndexScanBatch::no_results());
        }

        let (inner_results, new_buffered) =
            Self::pop_last_if_bigger_than_batch_size(configuration, inner_results);
        traversal.buffered = new_buffered;

        let array = Self::build_object_ids_for_level(configuration, &inner_results);
        let results = Self::coalesce_batches(configuration, &inner_results);
        let batch =
            Self::check_equality_if_necessary(configuration, results, &name, array);

        if traversal.consumed == self.0.len() {
            return IndexLevelActionResult::finished(batch);
        }
        IndexLevelActionResult {
            batch,
            new_state: Some(IndexLevelScanState::Scan(name, traversal)),
        }
    }

    fn check_equality_if_necessary(
        configuration: &IndexConfiguration,
        mut batch: IndexScanBatch,
        name: &str,
        array: ObjectIdArray,
    ) -> IndexScanBatch {
        if batch.columns.contains_key(name) {
            let lhs = array.object_ids();
            let rhs = batch.columns[name].object_ids();
            let is_equal = lhs
                .iter()
                .zip(rhs.iter())
                .enumerate()
                .filter_map(|(idx, (l, r))| (l == r).then(|| idx))
                .collect::<Vec<_>>();

            let mut new_results = HashMap::new();
            let keys = batch.columns.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let array = batch.columns.remove(&key).unwrap();
                let new_array = array
                    .object_ids()
                    .take_iter(is_equal.iter().copied().map(Some))
                    .collect::<UInt32Array>();
                let new_array = configuration
                    .object_id_encoding
                    .try_new_array(Arc::new(new_array))
                    .expect("TODO");
                new_results.insert(key, new_array);
            }

            IndexScanBatch {
                num_results: is_equal.len(),
                columns: new_results,
            }
        } else {
            batch.columns.insert(name.to_owned(), array);
            batch
        }
    }

    fn pop_last_if_bigger_than_batch_size(
        configuration: &IndexConfiguration,
        mut buffered: BufferedResults,
    ) -> (BufferedResults, Option<BufferedIndexScanBatch>) {
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
                array_builder.append_nulls(result.inner.num_results);
            } else {
                array_builder
                    .append_value_n(result.object_id.as_u32(), result.inner.num_results);
            }
        }
        configuration
            .object_id_encoding
            .try_new_array(Arc::new(array_builder.finish()))
            .expect("TODO")
    }

    fn coalesce_batches(
        configuration: &IndexConfiguration,
        inner_results: &BufferedResults,
    ) -> IndexScanBatch {
        if inner_results.results.is_empty() {
            return IndexScanBatch::no_results();
        }

        let mut result = HashMap::new();
        for field in inner_results.results[0].inner.columns.keys() {
            let iterator = inner_results
                .results
                .iter()
                .flat_map(|r| r.inner.columns[field].object_ids().values().iter())
                .copied();
            let u32_array = UInt32Array::from_iter_values(iterator);
            let array = configuration
                .object_id_encoding
                .try_new_array(Arc::new(u32_array))
                .expect("TODO");

            result.insert(field.to_string(), array);
        }

        IndexScanBatch {
            num_results: inner_results.num_results(),
            columns: result,
        }
    }
}

/// The state of the iterator traversal. As the traversal is the same for both scan-types, they
/// share the same struct.
#[derive(Debug, Clone)]
pub(super) struct IndexTraversal<TInnerState: Clone> {
    /// The predicate to scan for.
    predicate: Option<ObjectIdScanPredicate>,
    /// The inner states to set when moving to the next entry.
    default_state: TInnerState,
    /// Tracks how many entries have been consumed and should be skipped the next time.
    consumed: usize,
    /// The inner state.
    inner: TInnerState,
    /// The buffered result from the last iteration.
    buffered: Option<BufferedIndexScanBatch>,
}

impl<TInnerState: Clone> IndexTraversal<TInnerState> {
    fn try_collect_enough_results_for_batch<TLevel>(
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
            if buffered.inner.num_results == configuration.batch_size {
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
            if inner_result.batch.num_results > 0 {
                results.push(BufferedIndexScanBatch {
                    object_id: *oid,
                    inner: inner_result.batch,
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
    Scan(String, IndexTraversal<TInnerState>),
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
        name: String,
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
        Self::Scan(name, traversal)
    }
}

/// The result of an action on an index level.
#[derive(Debug)]
pub struct IndexLevelActionResult<TState> {
    /// The result of this level.
    pub batch: IndexScanBatch,
    /// The new state of the iterator at this level.
    pub new_state: Option<TState>,
}

impl<TState> IndexLevelActionResult<TState> {
    pub fn finished(result: IndexScanBatch) -> Self {
        Self {
            batch: result,
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
            IndexLevelScanState::Scan(name, traversal) => {
                self.scan_impl(configuration, name, traversal)
            }
        }
    }
}
