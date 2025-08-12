use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level_mapping::{
    IndexLevelActionResult, IndexLevelImpl,
};
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanBatch, IndexedQuad, ObjectIdScanPredicate,
};
use datafusion::arrow::array::UInt32Array;
use rdf_fusion_encoding::TermEncoding;
use std::collections::HashSet;
use std::sync::Arc;

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
    ) -> IndexLevelActionResult<IndexDataScanState> {
        let contained = match predicate {
            None => self.terms.len() > 0,
            Some(predicate) => self.terms.iter().any(|id| predicate.evaluate(*id)),
        };
        if contained {
            IndexLevelActionResult::finished(IndexScanBatch::single_empty_result())
        } else {
            IndexLevelActionResult::finished(IndexScanBatch::no_results())
        }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        name: String,
        predicate: Option<ObjectIdScanPredicate>,
        consumed: usize,
    ) -> IndexLevelActionResult<IndexDataScanState> {
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
        let array = UInt32Array::from_iter_values(iterator);
        let result = configuration
            .object_id_encoding
            .try_new_array(Arc::new(array))
            .expect("TODO");

        let batch = IndexScanBatch::new_with_column(name.clone(), result);
        let new_consumed = consumed + batch.num_results;
        if new_consumed == self.terms.len() {
            IndexLevelActionResult::finished(batch)
        } else {
            IndexLevelActionResult {
                batch,
                new_state: Some(IndexDataScanState::Scan {
                    name,
                    consumed: new_consumed,
                    predicate,
                }),
            }
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
    ) -> IndexLevelActionResult<Self::ScanState> {
        match state {
            IndexDataScanState::Traverse { predicate } => self.traverse_impl(predicate),
            IndexDataScanState::Scan {
                name,
                predicate,
                consumed,
            } => self.scan_impl(configuration, name, predicate, consumed),
        }
    }
}