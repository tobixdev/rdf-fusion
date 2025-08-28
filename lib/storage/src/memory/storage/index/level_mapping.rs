use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level::{IndexLevelImpl, ScanState};
use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstruction, IndexedQuad, ObjectIdScanPredicate,
};
use std::collections::HashMap;

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
            self,
            configuration,
            None,
            false,
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
        only_check_equality: bool,
        collector: &mut ScanCollector,
    ) -> (usize, Option<IndexLevelScanState<TInner::ScanState>>) {
        let matching_count = traversal.try_collect_enough_results_for_batch(
            self,
            configuration,
            Some(name.as_str()),
            only_check_equality,
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
}

impl<TContent: IndexLevelImpl> IndexLevelImpl for IndexLevel<TContent> {
    type ScanState<'idx> = IndexLevelScanState<'idx, TContent::ScanState<'idx>>;

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

    fn create_scan_state(
        &self,
        configuration: &IndexConfiguration,
        mut index_scan_instructions: Vec<IndexScanInstruction>,
    ) -> Self::ScanState<'_> {
        let instruction = index_scan_instructions
            .pop()
            .expect("There should always be a single instruction.");

        let iterator: TraversalIterator = match instruction.predicate().cloned() {
            None => Box::new(self.0.iter()),
            Some(ObjectIdScanPredicate::In(ids)) => {
                Box::new(ids.into_iter().filter_map(|id| self.0.get(&id)))
            }
            Some(predicate) => Box::new(
                self.0
                    .iter()
                    .filter(move |(id, _)| predicate.evaluate(**id))
                    .copied(),
            ),
        };
    }
}

type TraversalIterator<'idx> = Box<dyn Iterator<Item = EncodedObjectId> + 'idx>;

/// The state of the iterator traversal. As the traversal is the same for both scan-types, they
/// share the same struct.
#[derive(Debug, Clone)]
pub(super) struct IndexTraversal<'idx, TInnerState> {
    /// The current inner state. If this is `None`, the next entry should be scanned.
    inner_state: Option<TInnerState>,
    /// Used to create the inner state when moving to the next entry.
    inner_instructions: Vec<IndexScanInstruction>,
    /// The iterator over the entries in the index that match the given predicate.
    iterator: TraversalIterator<'idx>,
}

/// Represents the state of an action to execute.
#[derive(Debug, Clone)]
pub enum IndexLevelScanState<'idx, TInnerState: ScanState> {
    /// For this level, the iterator should traverse the entries and collect the results from the
    /// inner levels.
    Traverse(IndexTraversal<'idx, TInnerState>),
    /// Filter equaled elements from this level. This is used if the same variable appears multiple
    /// times in a single pattern.
    TraverseAndFilterEqual(String, IndexTraversal<'idx, TInnerState>),
    /// Same as [Self::Traverse] but also collects the elements from this level and returns them.
    Scan(String, IndexTraversal<'idx, TInnerState>),
}
