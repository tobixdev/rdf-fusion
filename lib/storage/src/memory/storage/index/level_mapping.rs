use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level::IndexLevelImpl;
use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanBatch, IndexedQuad, ObjectIdScanPredicate,
};
use datafusion::arrow::array::UInt32Array;
use rdf_fusion_encoding::object_id::ObjectIdArray;
use rdf_fusion_encoding::TermEncoding;
use std::collections::HashMap;
use std::iter::repeat_n;
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
        collector: &mut ScanCollector,
    ) -> (usize, Option<IndexLevelScanState<TInner::ScanState>>) {
        let matching_count = traversal.try_collect_enough_results_for_batch(
            &self,
            configuration,
            None,
            collector,
        );
        let max_consumed = traversal.max_consumed(self.0.len());

        if traversal.consumed == max_consumed {
            (matching_count, None)
        } else {
            (
                matching_count,
                Some(IndexLevelScanState::Traverse(traversal)),
            )
        }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        name: String,
        mut traversal: IndexTraversal<TInner::ScanState>,
        collector: &mut ScanCollector,
    ) -> (usize, Option<IndexLevelScanState<TInner::ScanState>>) {
        let matching_count = traversal.try_collect_enough_results_for_batch(
            &self,
            configuration,
            Some(name.as_str()),
            collector,
        );
        let max_consumed = traversal.max_consumed(self.0.len());

        if traversal.consumed == max_consumed {
            (matching_count, None)
        } else {
            (
                matching_count,
                Some(IndexLevelScanState::Scan(name, traversal)),
            )
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
}

impl<TInnerState: Clone> IndexTraversal<TInnerState> {
    pub fn max_consumed(&self, index_len: usize) -> usize {
        match &self.predicate {
            Some(ObjectIdScanPredicate::In(ids)) => ids.len(),
            _ => index_len,
        }
    }

    fn try_collect_enough_results_for_batch<TLevel>(
        &mut self,
        index: &IndexLevel<TLevel>,
        configuration: &IndexConfiguration,
        column: Option<&str>,
        collector: &mut ScanCollector,
    ) -> usize
    where
        TLevel: IndexLevelImpl<ScanState = TInnerState>,
    {
        match self.predicate.clone() {
            Some(ObjectIdScanPredicate::In(ids)) => self.try_collect_results_from_iter(
                ids.iter().filter_map(|id| index.0.get(id).map(|c| (id, c))),
                configuration,
                column,
                collector,
            ),
            _ => self.try_collect_results_from_iter(
                index.0.iter(),
                configuration,
                column,
                collector,
            ),
        }
    }

    fn try_collect_results_from_iter<
        'iter,
        InnerLevel: IndexLevelImpl<ScanState = TInnerState>,
        Iter: IntoIterator<Item = (&'iter EncodedObjectId, &'iter InnerLevel)>,
    >(
        &mut self,
        iter: Iter,
        configuration: &IndexConfiguration,
        column: Option<&str>,
        collector: &mut ScanCollector,
    ) -> usize
    where
        InnerLevel: 'iter,
    {
        let mut matching_count = 0;

        for (oid, next_level) in iter.into_iter().skip(self.consumed) {
            if let Some(predicate) = &self.predicate
                && !predicate.evaluate(*oid)
            {
                self.consumed += 1;
                continue;
            }

            let (inner_len, inner_state) =
                next_level.scan(configuration, self.inner.clone(), collector);
            matching_count += inner_len;

            if let Some(column) = column
                && inner_len > 0
            {
                collector.extend(column, repeat_n(oid.as_u32(), inner_len));
            }

            let inner_state = match inner_state {
                None => {
                    self.consumed += 1;
                    self.default_state.clone()
                },
                Some(inner_state) => inner_state,
            };
            self.inner = inner_state;

            if collector.batch_full() {
                break;
            }
        }

        matching_count
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
        };
        Self::Scan(name, traversal)
    }
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
        collector: &mut ScanCollector,
    ) -> (usize, Option<Self::ScanState>) {
        match state {
            IndexLevelScanState::Traverse(traversal) => {
                self.traverse_impl(configuration, traversal, collector)
            }
            IndexLevelScanState::Scan(name, traversal) => {
                self.scan_impl(configuration, name, traversal, collector)
            }
        }
    }
}
