use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level_mapping::{
    IndexLevelActionResult, IndexLevelImpl,
};
use crate::memory::storage::index::{
    IndexConfiguration, IndexedQuad, ObjectIdScanPredicate,
};
use datafusion::arrow::array::{Array, UInt32Array};
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
    Traverse {
        predicate: Option<ObjectIdScanPredicate>,
    },
    /// Scan the object ids in this level, only yielding the ids in `filter`.
    Scan {
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
            IndexLevelActionResult::finished(1, Some(vec![]))
        } else {
            IndexLevelActionResult::empty_finished()
        }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
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
        let new_consumed = consumed + result.object_ids().len();

        if new_consumed == self.terms.len() {
            IndexLevelActionResult::finished(new_consumed, Some(vec![result]))
        } else {
            IndexLevelActionResult {
                num_results: result.object_ids().len(),
                result: Some(vec![result]),
                new_state: Some(IndexDataScanState::Scan {
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
            IndexDataScanState::Traverse { predicate } => self.traverse_impl(predicate),
            IndexDataScanState::Scan {
                predicate,
                consumed,
            } => self.scan_impl(configuration, predicate, consumed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::storage::index::components::IndexComponent;
    use crate::memory::storage::index::hash_index::MemHashTripleIndex;
    use crate::memory::storage::index::{
        IndexComponents, IndexScanError, IndexScanInstruction, IndexScanInstructions,
    };
    use crate::memory::storage::VersionNumber;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;

    #[tokio::test]
    async fn insert_and_scan_triple() {
        let index = create_index();
        let quads = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        let mut iter = index
            .scan(
                IndexScanInstructions([
                    traverse(0),
                    traverse(1),
                    traverse(2),
                    traverse(3),
                ]),
                VersionNumber(1),
            )
            .await
            .unwrap();
        let result = iter.next();

        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 0); // Nothing is bound to a variable -> No outputs.
    }

    #[tokio::test]
    async fn scan_newer_index_version_err() {
        let index = create_index();
        let quads = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index.insert(&quads, VersionNumber(1)).await.unwrap();
        index.remove(&quads, VersionNumber(2)).await.unwrap();

        assert_eq!(
            index
                .scan(
                    IndexScanInstructions([scan(), scan(), scan(), scan()]),
                    VersionNumber(1)
                )
                .await
                .err(),
            Some(IndexScanError::UnexpectedIndexVersionNumber)
        );
    }

    #[tokio::test]
    async fn scan_subject_var() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), scan(), traverse(2), traverse(3)]),
            1,
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_predicate_var() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), scan(), traverse(3)]),
            1,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_object_var() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), traverse(2), scan()]),
            1,
            1,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_multi_vars() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), scan(), traverse(2), scan()]),
            2,
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_all_vars() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([scan(), scan(), scan(), scan()]),
            4,
            3,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_batches_for_batch_size() {
        let index = create_index_with_batch_size(10);
        let mut quads = Vec::new();
        for i in 0..25 {
            quads.push(IndexedQuad([eid(0), eid(1), eid(2), eid(i)]))
        }
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        // The lookup matches a single IndexData that will be scanned.
        run_batch_size_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), traverse(2), scan()]),
            &[10, 10, 5],
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_multi_level_batches_coalesce_results() {
        let index = create_index_with_batch_size(10);
        let mut quads = Vec::new();
        for i in 0..25 {
            quads.push(IndexedQuad([eid(0), eid(1), eid(i), eid(2)]))
        }
        index.insert(&quads, VersionNumber(1)).await.unwrap();

        // The lookup matches 25 different IndexLevels, each having exactly one data entry. The
        // batches should be combined into a single batch.
        run_batch_size_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), scan(), traverse(2)]),
            &[10, 10, 5],
            true,
        )
        .await;
    }

    #[tokio::test]
    async fn delete_triple_removes_it() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index.insert(&quads, VersionNumber(1)).await.unwrap();
        index.remove(&quads, VersionNumber(2)).await.unwrap();

        run_non_matching_test(
            index,
            IndexScanInstructions([scan(), scan(), scan(), scan()]),
        )
        .await;
    }

    #[tokio::test]
    async fn delete_triple_non_existing_ok() {
        let index = create_index();
        let quads = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        let result = index.remove(&quads, VersionNumber(1)).await;
        assert!(result.ok().is_some());
    }

    fn create_index() -> MemHashTripleIndex {
        create_index_with_batch_size(10)
    }

    fn create_index_with_batch_size(batch_size: usize) -> MemHashTripleIndex {
        let configuration = IndexConfiguration {
            batch_size,
            object_id_encoding: ObjectIdEncoding::new(4),
            components: IndexComponents::try_new([
                IndexComponent::GraphName,
                IndexComponent::Subject,
                IndexComponent::Predicate,
                IndexComponent::Object,
            ])
            .unwrap(),
        };
        MemHashTripleIndex::new(configuration)
    }

    fn traverse(id: u32) -> IndexScanInstruction {
        IndexScanInstruction::Traverse(Some(ObjectIdScanPredicate::In(HashSet::from([
            EncodedObjectId::from(id),
        ]))))
    }

    fn scan() -> IndexScanInstruction {
        IndexScanInstruction::Scan(None)
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
