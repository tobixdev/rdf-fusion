use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level::{IndexLevelImpl, ScanState};
use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstruction, IndexedQuad, ObjectIdScanPredicate,
};
use std::collections::{BTreeMap, HashSet};
use std::iter::repeat_n;

/// An index level is a mapping from [EncodedObjectId]. By traversing multiple index levels, users
/// can access the data in the index.
#[derive(Debug)]
pub struct IndexLevel<TInner: IndexLevelImpl>(BTreeMap<EncodedObjectId, TInner>);

impl<TInner: IndexLevelImpl> Default for IndexLevel<TInner> {
    fn default() -> Self {
        Self(BTreeMap::new())
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
}

impl<TInner: IndexLevelImpl> IndexLevelImpl for IndexLevel<TInner> {
    type ScanState<'idx>
        = IndexLevelScanState<'idx, TInner>
    where
        TInner: 'idx;

    fn insert(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> bool {
        let part = triple.0[cur_depth];
        let content = self.0.entry(part).or_insert_with(|| TInner::default());
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
        _configuration: &IndexConfiguration,
        mut index_scan_instructions: Vec<IndexScanInstruction>,
    ) -> Self::ScanState<'_> {
        let instruction = index_scan_instructions.remove(0);

        let iterator: TraversalIterator<TInner> = match instruction.predicate().cloned() {
            None => Box::new(self.0.iter()),
            Some(ObjectIdScanPredicate::In(ids)) => {
                Box::new(ids.into_iter().filter_map(|id| self.0.get_key_value(&id)))
            }
            Some(predicate) => Box::new(
                self.0
                    .iter()
                    .filter(move |(id, _)| predicate.evaluate(**id)),
            ),
        };

        let state_factory: Box<
            dyn Fn(EncodedObjectId) -> Vec<IndexScanInstruction> + Send,
        > = match &instruction {
            IndexScanInstruction::Scan(name, _) => {
                let name = name.clone();
                Box::new(move |oid| {
                    index_scan_instructions
                        .clone()
                        .into_iter()
                        .map(|inst| match inst {
                            IndexScanInstruction::Scan(inner_name, predicate)
                                if inner_name == name =>
                            {
                                if predicate.is_some() {
                                    todo!("Proper error handling")
                                }

                                IndexScanInstruction::Traverse(Some(
                                    ObjectIdScanPredicate::In(HashSet::from([oid])),
                                ))
                            }
                            inst => inst,
                        })
                        .collect()
                })
            }
            _ => Box::new(move |_| index_scan_instructions.clone()),
        };

        let traversal = IndexTraversal {
            state_factory,
            inner_state: None,
            iterator,
        };

        match instruction {
            IndexScanInstruction::Traverse(_) => IndexLevelScanState::Traverse(traversal),
            IndexScanInstruction::Scan(name, _) => {
                IndexLevelScanState::Scan(name, traversal)
            }
        }
    }
}

type TraversalIterator<'idx, TInner> =
    Box<dyn Iterator<Item = (&'idx EncodedObjectId, &'idx TInner)> + 'idx + Send>;

/// The state of the iterator traversal. As the traversal is the same for both scan-types, they
/// share the same struct.
pub(super) struct IndexTraversal<'idx, TInner: IndexLevelImpl> {
    /// The current inner state. If this is `None`, the next entry should be scanned.
    inner_state: Option<(EncodedObjectId, TInner::ScanState<'idx>)>,
    /// Used to create the inner instructions when the next entry is scanned.
    state_factory: Box<dyn Fn(EncodedObjectId) -> Vec<IndexScanInstruction> + Send>,
    /// The iterator over the entries in the index that match the given predicate.
    iterator: TraversalIterator<'idx, TInner>,
}

impl<'idx, TInner: IndexLevelImpl> IndexTraversal<'idx, TInner> {
    fn next_state(
        &mut self,
        configuration: &IndexConfiguration,
    ) -> Option<(EncodedObjectId, TInner::ScanState<'idx>)> {
        if let Some(state) = self.inner_state.take() {
            return Some(state);
        }
        let (oid, inner) = self.iterator.next()?;
        let instructions = (self.state_factory)(*oid);
        let state = inner.create_scan_state(configuration, instructions);
        Some((*oid, state))
    }
}

/// Represents the state of an action to execute.
pub enum IndexLevelScanState<'idx, TContent: IndexLevelImpl> {
    /// For this level, the iterator should traverse the entries and collect the results from the
    /// inner levels.
    Traverse(IndexTraversal<'idx, TContent>),
    /// Same as [Self::Traverse] but also collects the elements from this level and returns them.
    Scan(String, IndexTraversal<'idx, TContent>),
}

impl<'idx, TContent: IndexLevelImpl> ScanState for IndexLevelScanState<'idx, TContent> {
    fn drive_scan(
        self,
        configuration: &IndexConfiguration,
        collector: &mut ScanCollector,
    ) -> (usize, Option<Self>)
    where
        Self: Sized,
    {
        match self {
            IndexLevelScanState::Traverse(mut traversal) => {
                let mut count = 0;
                while let Some((oid, state)) = traversal.next_state(configuration) {
                    let (this_count, state) = state.drive_scan(configuration, collector);
                    count += this_count;

                    if collector.batch_full() {
                        let traversal = IndexTraversal {
                            inner_state: state.map(|state| (oid, state)),
                            state_factory: traversal.state_factory,
                            iterator: traversal.iterator,
                        };
                        return (count, Some(IndexLevelScanState::Traverse(traversal)));
                    }
                }
                (count, None)
            }
            IndexLevelScanState::Scan(name, mut traversal) => {
                let mut count = 0;
                while let Some((oid, state)) = traversal.next_state(configuration) {
                    let (this_count, state) = state.drive_scan(configuration, collector);
                    count += this_count;
                    collector.extend(&name, repeat_n(oid.as_u32(), this_count));

                    if collector.batch_full() {
                        let traversal = IndexTraversal {
                            inner_state: state.map(|state| (oid, state)),
                            state_factory: traversal.state_factory,
                            iterator: traversal.iterator,
                        };
                        return (count, Some(IndexLevelScanState::Scan(name, traversal)));
                    }
                }
                (count, None)
            }
        }
    }
}
