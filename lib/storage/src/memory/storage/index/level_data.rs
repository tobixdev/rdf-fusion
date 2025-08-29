use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level::{IndexLevelImpl, ScanState};
use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstruction, IndexedQuad, ObjectIdScanPredicate,
};
use std::collections::BTreeSet;

/// Holds the data for the last index level.
#[derive(Debug, Default)]
pub struct IndexData {
    /// The object ids already seen.
    terms: BTreeSet<EncodedObjectId>,
}

impl IndexLevelImpl for IndexData {
    type ScanState<'idx> = IndexDataScanState<'idx>;

    fn insert(
        &mut self,
        _configuration: &IndexConfiguration,
        quad: &IndexedQuad,
        cur_depth: usize,
    ) -> bool {
        let part = quad.0[cur_depth];
        self.terms.insert(part)
    }

    fn remove(
        &mut self,
        _configuration: &IndexConfiguration,
        quad: &IndexedQuad,
        cur_depth: usize,
    ) -> bool {
        let part = quad.0[cur_depth];
        self.terms.remove(&part)
    }

    fn num_triples(&self) -> usize {
        self.terms.len()
    }

    fn create_scan_state(
        &self,
        configuration: &IndexConfiguration,
        mut index_scan_instructions: Vec<IndexScanInstruction>,
    ) -> Self::ScanState<'_> {
        let instruction = index_scan_instructions
            .pop()
            .expect("There should always be a single instruction.");
        debug_assert!(index_scan_instructions.is_empty());

        let iterator: TraversalIterator = match instruction.predicate().cloned() {
            None => Box::new(self.terms.iter().copied()),
            Some(ObjectIdScanPredicate::In(ids)) => {
                Box::new(ids.into_iter().filter(|id| self.terms.contains(id)))
            }
            Some(predicate) => Box::new(
                self.terms
                    .iter()
                    .filter(move |id| predicate.evaluate(**id))
                    .copied(),
            ),
        };

        match instruction {
            IndexScanInstruction::Traverse(_) => {
                IndexDataScanState::Traverse { iterator }
            }
            IndexScanInstruction::Scan(_, _) => {
                IndexDataScanState::Scan {
                    result_idx: 3, // Data is always the last level.
                    batch_size: configuration.batch_size,
                    iterator,
                }
            }
        }
    }
}

/// An iterator over object ids is used for traversing the index.
type TraversalIterator<'idx> = Box<dyn Iterator<Item = EncodedObjectId> + 'idx + Send>;

/// Represents the state of scanning an [IndexData] instance.
pub enum IndexDataScanState<'idx> {
    /// Look up the object id in the index. If a single item is that is contained in `filter` is
    /// found, the lookup is successful.
    Traverse { iterator: TraversalIterator<'idx> },
    /// Scan the object ids in this level, only yielding the ids in `filter`.
    Scan {
        result_idx: usize,
        batch_size: usize,
        iterator: TraversalIterator<'idx>,
    },
}

impl ScanState for IndexDataScanState<'_> {
    fn drive_scan(
        self,
        _configuration: &IndexConfiguration,
        collector: &mut ScanCollector,
    ) -> (usize, Option<Self>) {
        match self {
            // During traversal, check how many items are in the iterator.
            IndexDataScanState::Traverse { iterator } => (iterator.count(), None),
            // During scan, yield the next batch of ids.
            IndexDataScanState::Scan {
                result_idx,
                batch_size,
                mut iterator,
            } => {
                let old_results = collector.num_results(result_idx);
                let batch_iter = iterator.by_ref().take(batch_size).map(|id| id.as_u32());
                collector.extend(result_idx, batch_iter);

                let added_elements = collector.num_results(result_idx) - old_results;
                if added_elements < batch_size {
                    (added_elements, None)
                } else {
                    (
                        batch_size,
                        Some(IndexDataScanState::Scan {
                            result_idx,
                            batch_size,
                            iterator,
                        }),
                    )
                }
            }
        }
    }
}
