use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level_mapping::{
    IndexLevelActionResult, IndexLevelImpl,
};
use crate::memory::storage::index::{IndexConfiguration, IndexedQuad};
use datafusion::arrow::array::{Array, UInt32Array};
use rdf_fusion_encoding::object_id::ObjectIdArray;
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
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
    LookupOnly { filter: HashSet<EncodedObjectId> },
    /// Look up the object id in the index. If a single item is that is not contained in `filter` is
    /// found, the lookup is successful.
    LookupExcept { filter: HashSet<EncodedObjectId> },
    /// Scan the object ids in this level, only yielding the ids in `filter`.
    ScanOnly {
        filter: HashSet<EncodedObjectId>,
        consumed: usize,
    },
    /// Scan the object ids in this level, only yielding the ids not in `filter`.
    ScanExcept {
        consumed: usize,
        filter: HashSet<EncodedObjectId>,
    },
}

impl IndexData {
    /// Implements the lookup only action by checking whether any of the object ids is contained
    /// in the index.
    fn lookup_only_impl(
        &self,
        object_ids: HashSet<EncodedObjectId>,
    ) -> IndexLevelActionResult<IndexDataScanState> {
        let contained = object_ids.iter().any(|id| self.terms.contains(id));
        if contained {
            IndexLevelActionResult::finished(1, Some(vec![]))
        } else {
            IndexLevelActionResult::empty_finished()
        }
    }

    /// Implements the lookup action by checking whether any object id is contained in the index
    /// that is not contained in `filter`.
    fn lookup_except_impl(
        &self,
        object_ids: HashSet<EncodedObjectId>,
    ) -> IndexLevelActionResult<IndexDataScanState> {
        // Fast path if more distinct items exist in this level.
        if self.terms.len() > object_ids.len() {
            return IndexLevelActionResult::finished(1, Some(vec![]));
        }

        let contained = self.terms.iter().any(|id| !object_ids.contains(id));
        if contained {
            IndexLevelActionResult::finished(1, Some(vec![]))
        } else {
            IndexLevelActionResult::empty_finished()
        }
    }

    fn scan_only_impl(
        &self,
        configuration: &IndexConfiguration,
        filter: HashSet<EncodedObjectId>,
        consumed: usize,
    ) -> IndexLevelActionResult<IndexDataScanState> {
        let (result, new_consumed) =
            self.scan_impl(configuration, consumed, |oid| filter.contains(oid));

        if new_consumed == self.terms.len() {
            IndexLevelActionResult::finished(new_consumed, Some(vec![result]))
        } else {
            IndexLevelActionResult {
                num_results: result.object_ids().len(),
                result: Some(vec![result]),
                new_state: Some(IndexDataScanState::ScanOnly {
                    consumed: new_consumed,
                    filter,
                }),
            }
        }
    }

    fn scan_except_impl(
        &self,
        configuration: &IndexConfiguration,
        filter: HashSet<EncodedObjectId>,
        consumed: usize,
    ) -> IndexLevelActionResult<IndexDataScanState> {
        let (result, new_consumed) =
            self.scan_impl(configuration, consumed, |oid| !filter.contains(oid));

        if new_consumed == self.terms.len() {
            IndexLevelActionResult::finished(new_consumed, Some(vec![result]))
        } else {
            IndexLevelActionResult {
                num_results: result.object_ids().len(),
                result: Some(vec![result]),
                new_state: Some(IndexDataScanState::ScanExcept {
                    consumed: new_consumed,
                    filter,
                }),
            }
        }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        consumed: usize,
        filter: impl Fn(&EncodedObjectId) -> bool,
    ) -> (ObjectIdArray, usize) {
        let iterator = self
            .terms
            .iter()
            .skip(consumed)
            .take(configuration.batch_size)
            .filter(|id| filter(*id))
            .map(|id| id.as_u32());
        let array = UInt32Array::from_iter_values(iterator);
        let result = configuration
            .object_id_encoding
            .try_new_array(Arc::new(array))
            .expect("TODO");
        let new_consumed = consumed + result.object_ids().len();
        (result, new_consumed)
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
    ) {
        let part = quad.0[cur_depth];
        self.terms.remove(&part);
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
            IndexDataScanState::LookupOnly { filter } => self.lookup_only_impl(filter),
            IndexDataScanState::LookupExcept { filter } => {
                self.lookup_except_impl(filter)
            }
            IndexDataScanState::ScanOnly { filter, consumed } => {
                self.scan_only_impl(configuration, filter, consumed)
            }
            IndexDataScanState::ScanExcept { filter, consumed } => {
                self.scan_except_filter(configuration, filter, consumed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::storage::index::hash_index::MemHashTripleIndex;
    use crate::memory::storage::index::{IndexComponent, IndexComponents, IndexScanError, IndexScanInstruction, IndexScanInstructions};
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use crate::memory::encoding::EncodedTermPattern;
    use crate::memory::storage::VersionNumber;

    #[tokio::test]
    async fn insert_and_scan_triple() {
        let index = create_index();
        let triples = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index.insert(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexScanInstructions([
            IndexScanInstruction::ObjectId(eid(0)),
            EncodedTermPattern::ObjectId(eid(1)),
            EncodedTermPattern::ObjectId(eid(2)),
            EncodedTermPattern::ObjectId(eid(3)),
        ]);
        let mut iter = index.scan(lookup, VersionNumber(1)).await.unwrap();
        let result = iter.next();

        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 0); // Nothing is bound to a variable -> No outputs.
    }

    #[tokio::test]
    async fn scan_newer_index_version_err() {
        let index = create_index();
        let triples = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index
            .insert(&triples, VersionNumber(1))
            .await
            .unwrap();
        index
            .delete(&triples, VersionNumber(2))
            .await
            .unwrap();

        let result = index
            .scan(
                IndexScanInstructions([EncodedTermPattern::Variable; 4]),
                VersionNumber(1),
            )
            .await;

        assert_eq!(
            result.err(),
            Some(IndexScanError::UnexpectedIndexVersionNumber)
        );
    }

    #[tokio::test]
    async fn scan_subject_var() {
        let index = create_index();
        let triples = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();

        let lookup = IndexScanInstructions([
            EncodedTermPattern::ObjectId(eid(0)),
            EncodedTermPattern::Variable,
            EncodedTermPattern::ObjectId(eid(2)),
            EncodedTermPattern::ObjectId(eid(3)),
        ]);

        run_matching_test(index, lookup, 1, 2).await;
    }

    #[tokio::test]
    async fn scan_predicate_var() {
        let index = create_index();
        let triples = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();

        let lookup = IndexScanInstructions([
            EncodedTermPattern::ObjectId(eid(0)),
            EncodedTermPattern::ObjectId(eid(1)),
            EncodedTermPattern::Variable,
            EncodedTermPattern::ObjectId(eid(3)),
        ]);

        run_matching_test(index, lookup, 1, 1).await;
    }

    #[tokio::test]
    async fn scan_object_var() {
        let index = create_index();
        let triples = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();

        let lookup = IndexScanInstructions([
            EncodedTermPattern::ObjectId(eid(0)),
            EncodedTermPattern::ObjectId(eid(1)),
            EncodedTermPattern::ObjectId(eid(2)),
            EncodedTermPattern::Variable,
        ]);

        run_matching_test(index, lookup, 1, 1).await;
    }

    #[tokio::test]
    async fn scan_multi_vars() {
        let index = create_index();
        let triples = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();

        let lookup = IndexScanInstructions([
            EncodedTermPattern::ObjectId(eid(0)),
            EncodedTermPattern::Variable,
            EncodedTermPattern::ObjectId(eid(2)),
            EncodedTermPattern::Variable,
        ]);

        run_matching_test(index, lookup, 2, 2).await;
    }

    #[tokio::test]
    async fn scan_all_vars() {
        let index = create_index();
        let triples = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();

        let lookup = IndexScanInstructions([EncodedTermPattern::Variable; 4]);

        run_matching_test(index, lookup, 4, 3).await;
    }

    #[tokio::test]
    async fn scan_batches_for_batch_size() {
        let index = create_index_with_batch_size(10);
        let mut triples = Vec::new();
        for i in 0..25 {
            triples.push(IndexedQuad([eid(0), eid(1), eid(2), eid(i)]))
        }
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();

        // The lookup matches a single IndexData that will be scanned.
        let lookup = IndexScanInstructions([
            EncodedTermPattern::ObjectId(eid(0)),
            EncodedTermPattern::ObjectId(eid(1)),
            EncodedTermPattern::ObjectId(eid(2)),
            EncodedTermPattern::Variable,
        ]);

        run_batch_size_test(index, lookup, &[10, 10, 5], true).await;
    }

    #[tokio::test]
    async fn scan_multi_level_batches_coalesce_results() {
        let index = create_index_with_batch_size(10);
        let mut triples = Vec::new();
        for i in 0..25 {
            triples.push(IndexedQuad([eid(0), eid(1), eid(i), eid(2)]))
        }
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();

        // The lookup matches 25 different IndexLevels, each having exactly one data entry. The
        // batches should be combined into a single batch.
        let lookup = IndexScanInstructions([
            EncodedTermPattern::ObjectId(eid(0)),
            EncodedTermPattern::ObjectId(eid(1)),
            EncodedTermPattern::Variable,
            EncodedTermPattern::ObjectId(eid(2)),
        ]);

        run_batch_size_test(index, lookup, &[10, 10, 5], true).await;
    }

    #[tokio::test]
    async fn delete_triple_removes_it() {
        let index = create_index();
        let triples = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index
            .apply_change_set(&triples, &[], VersionNumber(1))
            .await
            .unwrap();
        index
            .apply_change_set(&[], &triples, VersionNumber(2))
            .await
            .unwrap();

        run_non_matching_test(
            index,
            IndexScanInstructions([EncodedTermPattern::Variable; 4]),
        )
        .await;
    }

    #[tokio::test]
    async fn delete_triple_non_existing_err() {
        let index = create_index();
        let triples = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        let result = index
            .apply_change_set(&[], &triples, VersionNumber(1))
            .await;
        assert_eq!(
            result.err(),
            Some(IndexUpdateError::IndexDeletionError(
                IndexDeletionError::NonExistingTriple
            ))
        );
    }

    fn create_index() -> MemHashTripleIndex {
        create_index_with_batch_size(10)
    }

    fn create_index_with_batch_size(batch_size: usize) -> MemHashTripleIndex {
        let object_id_encoding = ObjectIdEncoding::new(4);
        let content = IndexContent::default();
        let index = MemHashTripleIndex {
            configuration: IndexConfiguration {
                batch_size,
                object_id_encoding,
                components: IndexComponents::try_new([
                    IndexComponent::GraphName,
                    IndexComponent::Subject,
                    IndexComponent::Predicate,
                    IndexComponent::Object,
                ])
                .unwrap(),
            },
            content: Arc::new(RwLock::new(content)),
        };
        index
    }

    fn eid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }

    async fn run_non_matching_test(
        index: MemHashTripleIndex,
        lookup: IndexScanInstructions,
    ) {
        let results = index
            .scan(lookup, VersionNumber(100)) // Use 100 for never violating the version check.
            .await
            .unwrap()
            .next();
        assert!(
            results.is_none(),
            "Expected no results in non-matching test."
        );
    }

    async fn run_matching_test(
        index: MemHashTripleIndex,
        lookup: IndexScanInstructions,
        expected_columns: usize,
        expected_rows: usize,
    ) {
        let results: Vec<_> = index
            .scan(lookup, VersionNumber(100)) // Use 100 for never violating the version check.
            .await
            .unwrap()
            .next()
            .unwrap();

        assert_eq!(results.len(), expected_columns);
        for result in results {
            assert_eq!(result.array().len(), expected_rows);
        }
    }

    async fn run_batch_size_test(
        index: MemHashTripleIndex,
        lookup: IndexScanInstructions,
        expected_batch_sizes: &[usize],
        ordered: bool,
    ) {
        let mut batch_sizes: Vec<_> = index
            .scan(lookup, VersionNumber(1))
            .await
            .unwrap()
            .map(|arr| arr[0].array().len())
            .collect();

        if ordered {
            assert_eq!(batch_sizes, expected_batch_sizes);
        } else {
            let mut expected_batch_sizes = expected_batch_sizes.to_vec();
            batch_sizes.sort();
            expected_batch_sizes.sort();

            assert_eq!(batch_sizes, expected_batch_sizes);
        }
    }
}
