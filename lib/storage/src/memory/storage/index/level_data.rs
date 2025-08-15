use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level::IndexLevelImpl;
use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{
    IndexConfiguration, IndexedQuad, ObjectIdScanPredicate,
};
use std::cmp::min;
use std::collections::HashSet;

/// Holds the data for the last index level.
#[derive(Debug, Default)]
pub struct IndexData {
    /// The object ids already seen.
    terms: HashSet<EncodedObjectId>,
}

/// Represents the state of scanning an [IndexData] instance.
#[derive(Debug, Clone)]
pub enum IndexDataScanState {
    /// Look up the object id in the index. If a single item is that is contained in `filter` is
    /// found, the lookup is successful.
    Traverse {
        predicate: Option<ObjectIdScanPredicate>,
    },
    /// Scan the object ids in this level, only yielding the ids in `filter`.
    Scan {
        name: String,
        predicate: Option<ObjectIdScanPredicate>,
        consumed: usize,
    },
}

impl IndexData {
    /// Implements the lookup only action by checking whether any of the object ids is contained
    /// in the index.
    fn traverse_impl(
        &self,
        predicate: Option<ObjectIdScanPredicate>,
    ) -> (usize, Option<IndexDataScanState>) {
        let contained = match predicate {
            None => !self.terms.is_empty(),
            Some(predicate) => self.terms.iter().any(|id| predicate.evaluate(*id)),
        };
        if contained { (1, None) } else { (0, None) }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        name: String,
        predicate: Option<ObjectIdScanPredicate>,
        consumed: usize,
        collector: &mut ScanCollector,
    ) -> (usize, Option<IndexDataScanState>) {
        // TODO: Specialize predicate

        let old_results = collector.num_results(&name);
        let iterator = self
            .terms
            .iter()
            .skip(consumed)
            .take(configuration.batch_size)
            .filter(|id| match &predicate {
                None => true,
                Some(predicate) => predicate.evaluate(**id),
            })
            .map(|id| id.as_u32());
        collector.extend(name.as_str(), iterator);

        let added_elements = collector.num_results(&name) - old_results;
        let new_consumed = min(consumed + configuration.batch_size, self.terms.len());
        if new_consumed == self.terms.len() {
            (added_elements, None)
        } else {
            (
                added_elements,
                Some(IndexDataScanState::Scan {
                    name,
                    consumed: new_consumed,
                    predicate,
                }),
            )
        }
    }
}

impl IndexLevelImpl for IndexData {
    type ScanState = IndexDataScanState;

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

    fn scan(
        &self,
        configuration: &IndexConfiguration,
        state: Self::ScanState,
        collector: &mut ScanCollector,
    ) -> (usize, Option<Self::ScanState>) {
        match state {
            IndexDataScanState::Traverse { predicate } => self.traverse_impl(predicate),
            IndexDataScanState::Scan {
                name,
                predicate,
                consumed,
            } => self.scan_impl(configuration, name, predicate, consumed, collector),
        }
    }
}
