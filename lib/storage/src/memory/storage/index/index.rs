use crate::memory::encoding::EncodedObjectIdPattern;
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::error::{IndexDeletionError, IndexUpdateError};
use crate::memory::storage::index::level::{
    create_state_for_level, IndexLevel, IndexLevelActionResult, IndexLevelImpl,
    IndexLevelScanState,
};
use crate::memory::storage::index::IndexConfiguration;
use crate::memory::storage::VersionNumber;
use datafusion::arrow::array::{Array, UInt32Array};
use datafusion::common::exec_err;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::{ObjectIdArray};
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// Represents the index.
type Index = IndexLevel<IndexLevel<IndexData>>;

/// The full type of the index scan iterator.
type IndexScanState = IndexLevelScanState<IndexLevelScanState<IndexDataScanState>>;

/// Holds the data for the last index level.
struct IndexData {
    /// The object ids. The index tries to keep the object ids in batches of `batch_size`.
    arrays: Vec<ObjectIdArray>,
    /// The currently building object ids.
    building: Vec<EncodedObjectId>,
}

#[derive(Debug, Clone)]
pub struct IndexLookup(pub [EncodedObjectIdPattern; 3]);

#[derive(Debug, Clone)]
pub struct IndexedTriple(pub [EncodedObjectId; 3]);

struct IndexContent {
    /// The version that this index reflects.
    version: VersionNumber,
    /// The index.
    index: Index,
}

pub struct MemHashTripleIndex {
    /// The index content.
    content: Arc<RwLock<IndexContent>>,
    /// The configuration of the index.
    configuration: Arc<IndexConfiguration>,
}

impl MemHashTripleIndex {
    /// Performs a lookup in the index and returns a list of object arrays.
    ///
    /// See [MemHashTripleIndexIterator] for more information.
    pub async fn lookup(
        &self,
        lookup: IndexLookup,
        version_number: VersionNumber,
    ) -> DFResult<MemHashTripleIndexIterator> {
        let lock = self.content.clone().read_owned().await;
        if lock.version > version_number {
            return exec_err!("Index is already past the inquired version.");
        }
        Ok(MemHashTripleIndexIterator::new(
            lock,
            self.configuration.clone(),
            lookup,
        ))
    }

    pub async fn update(
        &self,
        to_insert: &[IndexedTriple],
        to_delete: &[IndexedTriple],
        version_number: VersionNumber,
    ) -> Result<(), IndexUpdateError> {
        let mut index = self.content.write().await;

        for triple in to_insert {
            index.index.insert_triple(&self.configuration, triple, 0);
        }

        for triple in to_delete {
            index.index.delete_triple(triple, 0)?;
        }
        index.version = version_number;

        Ok(())
    }
}

/// An iterator that traverses the index and returns results.
///
/// # Batch Size
///
/// The iterator *tries* to adhere to the given batch size. However, the iterator may return smaller
/// batches in an attempt to return a full batch in the next iteration. Given the example below,
/// the iterator will return batches in the following sizes: `[8192] [8192] [6000] [8192]`.
/// So while smaller batches are coalesced into a single larger batch, the iterator returns a batch
/// with 6000 elements to "re-align" with entire batch sizes.
///
/// Index Lookup: `A ?var1 ?var2`
///
/// Index            Array Sizes
/// A -> B -> Data: `[8192] [8192] [5000]`
/// A -> C -> Data: `[1000]`
/// A -> D -> Data: `[8192]`
///
/// # Patterns Without Variables
///
/// If no variable is given (and only term patterns) the iterator will return a single item with an
/// empty vector. If no triple matches the pattern, then `None` will be returned.
pub struct MemHashTripleIndexIterator {
    /// The iterator holds a read lock on the entire index such that another transaction cannot
    /// delete data from the index during iteration.
    index: OwnedRwLockReadGuard<IndexContent>,
    /// Additional context necessary for the iteration.
    configuration: Arc<IndexConfiguration>,
    /// The states of the individual levels.
    state: Option<IndexScanState>,
}

impl MemHashTripleIndexIterator {
    /// Creates a new [MemHashTripleIndexIterator].
    fn new(
        index: OwnedRwLockReadGuard<IndexContent>,
        configuration: Arc<IndexConfiguration>,
        lookup: IndexLookup,
    ) -> Self {
        let state = build_state(lookup.0);
        Self {
            index,
            configuration,
            state: Some(state),
        }
    }
}

fn build_state(patterns: [EncodedObjectIdPattern; 3]) -> IndexScanState {
    let data = create_state_for_data(patterns[2]);
    let first_level = create_state_for_level(patterns[1], data);
    create_state_for_level(patterns[0], first_level)
}

fn create_state_for_data(pattern: EncodedObjectIdPattern) -> IndexDataScanState {
    match pattern {
        EncodedObjectIdPattern::ObjectId(object_id) => {
            IndexDataScanState::Lookup { object_id }
        }
        EncodedObjectIdPattern::Variable => IndexDataScanState::Scan { consumed: 0 },
    }
}

impl Iterator for MemHashTripleIndexIterator {
    type Item = Vec<ObjectIdArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(state) = self.state.take() else {
            return None;
        };

        let result = self.index.index.scan(&self.configuration, state);
        self.state = result.new_state;
        result.result
    }
}

/// Represents the state of scanning an [IndexData] instance.
#[derive(Debug, Clone)]
pub enum IndexDataScanState {
    Lookup {
        /// The object id to look up.
        object_id: EncodedObjectId,
    },
    Scan {
        /// Tracks how many arrays have been consumed and should be skipped the next time.
        consumed: usize,
    },
}

impl IndexData {
    /// Implements the lookup action by scanning all arrays for the object id.
    fn lookup_impl(
        &self,
        object_id: EncodedObjectId,
    ) -> IndexLevelActionResult<IndexDataScanState> {
        for array in &self.arrays {
            let contains = array
                .object_ids()
                .values()
                .iter()
                .any(|arr_id| EncodedObjectId::from(*arr_id) == object_id);
            if contains {
                return IndexLevelActionResult::finished(1, Some(vec![]));
            }
        }

        let contains = self.building.iter().any(|arr_id| *arr_id == object_id);
        if contains {
            IndexLevelActionResult::finished(1, Some(vec![]))
        } else {
            IndexLevelActionResult::empty_finished()
        }
    }

    fn scan_impl(
        &self,
        configuration: &IndexConfiguration,
        consumed: usize,
    ) -> IndexLevelActionResult<IndexDataScanState> {
        if consumed < self.arrays.len() {
            return IndexLevelActionResult {
                num_results: self.arrays[consumed].object_ids().len(),
                result: Some(vec![self.arrays[consumed].clone()]),
                new_state: Some(IndexDataScanState::Scan {
                    consumed: consumed + 1,
                }),
            };
        }

        if consumed == self.arrays.len() {
            let iterator = self.building.iter().map(|id| id.as_u32());
            let uint_array = UInt32Array::from_iter_values(iterator);

            let array = configuration
                .object_id_encoding
                .try_new_array(Arc::new(uint_array))
                .expect("Failed to create array.");

            return IndexLevelActionResult::finished(
                self.building.len(),
                Some(vec![array]),
            );
        }

        unreachable!("Should have returned an empty state earlier.")
    }
}

impl IndexLevelImpl for IndexData {
    type ScanState = IndexDataScanState;

    fn create_empty() -> Self {
        Self {
            arrays: Vec::new(),
            building: Vec::new(),
        }
    }

    fn insert_triple(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedTriple,
        cur_depth: usize,
    ) {
        let part = triple.0[cur_depth];
        self.building.push(part);

        if self.building.len() == configuration.batch_size {
            let object_ids = self.building.iter().map(|id| id.as_u32());
            let u32_array = UInt32Array::from_iter_values(object_ids);
            let object_id_array = configuration
                .object_id_encoding
                .try_new_array(Arc::new(u32_array))
                .expect("Failed to create array.");
            self.arrays.push(object_id_array);
            self.building.clear();
        }
    }

    fn delete_triple(
        &mut self,
        _triple: &IndexedTriple,
        _cur_depth: usize,
    ) -> Result<(), IndexDeletionError> {
        todo!()
    }

    fn num_triples(&self) -> usize {
        self.arrays.iter().map(|a| a.array().len()).sum()
    }

    fn scan(
        &self,
        configuration: &IndexConfiguration,
        state: Self::ScanState,
    ) -> IndexLevelActionResult<Self::ScanState> {
        match state {
            IndexDataScanState::Lookup { object_id } => self.lookup_impl(object_id),
            IndexDataScanState::Scan { consumed } => {
                self.scan_impl(configuration, consumed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::storage::index::{IndexComponent, IndexComponents};
    use datafusion::arrow::array::UInt32Array;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use rdf_fusion_encoding::TermEncoding;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn insert_and_lookup_triple() {
        let index = create_index();
        let triples = vec![IndexedTriple([eid(1), eid(2), eid(3)])];
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::ObjectId(eid(3)),
        ]);
        let mut iter = index.lookup(lookup, VersionNumber(1)).await.unwrap();
        let result = iter.next();

        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 0); // Nothing is bound to a variable -> No outputs.
    }

    #[tokio::test]
    async fn lookup_subject_var() {
        let index = create_index();
        let triples = vec![
            IndexedTriple([eid(1), eid(2), eid(3)]),
            IndexedTriple([eid(1), eid(4), eid(5)]),
            IndexedTriple([eid(6), eid(2), eid(3)]),
        ];
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::ObjectId(eid(3)),
        ]);

        run_matching_test(index, lookup, 1, 2).await;
    }

    #[tokio::test]
    async fn lookup_predicate_var() {
        let index = create_index();
        let triples = vec![
            IndexedTriple([eid(1), eid(2), eid(3)]),
            IndexedTriple([eid(1), eid(4), eid(5)]),
            IndexedTriple([eid(6), eid(2), eid(3)]),
        ];
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(3)),
        ]);

        run_matching_test(index, lookup, 1, 1).await;
    }

    #[tokio::test]
    async fn lookup_object_var() {
        let index = create_index();
        let triples = vec![
            IndexedTriple([eid(1), eid(2), eid(3)]),
            IndexedTriple([eid(1), eid(4), eid(5)]),
            IndexedTriple([eid(6), eid(2), eid(3)]),
        ];
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::Variable,
        ]);

        run_matching_test(index, lookup, 1, 1).await;
    }

    #[tokio::test]
    async fn lookup_multi_var() {
        let index = create_index();
        let triples = vec![
            IndexedTriple([eid(1), eid(2), eid(3)]),
            IndexedTriple([eid(1), eid(4), eid(5)]),
            IndexedTriple([eid(6), eid(2), eid(3)]),
        ];
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::Variable,
        ]);

        run_matching_test(index, lookup, 2, 2).await;
    }

    #[tokio::test]
    async fn lookup_all_var() {
        let index = create_index();
        let triples = vec![
            IndexedTriple([eid(1), eid(2), eid(3)]),
            IndexedTriple([eid(1), eid(4), eid(5)]),
            IndexedTriple([eid(6), eid(2), eid(3)]),
        ];
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([EncodedObjectIdPattern::Variable; 3]);

        run_matching_test(index, lookup, 3, 3).await;
    }

    #[tokio::test]
    async fn scan_batches_for_batch_size() {
        let index = create_index_with_batch_size(10);
        let mut triples = Vec::new();
        for i in 0..25 {
            triples.push(IndexedTriple([eid(1), eid(2), eid(i)]))
        }
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        // The lookup matches a single IndexData that will be scanned.
        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::Variable,
        ]);

        run_batch_size_test(index, lookup, 10, &[10, 10, 5], true).await;
    }

    #[tokio::test]
    async fn scan_multi_level_batches_coalesce_results() {
        let index = create_index_with_batch_size(10);
        let mut triples = Vec::new();
        for i in 0..25 {
            triples.push(IndexedTriple([eid(1), eid(i), eid(2)]))
        }
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        // The lookup matches 25 different IndexLevels, each having exactly one data entry. The
        // batches should be combined into a single batch.
        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(2)),
        ]);

        run_batch_size_test(index, lookup, 10, &[10, 10, 5], true).await;
    }

    #[tokio::test]
    async fn delete_triple_removes_it() {
        let index = create_index();

        let triple = IndexedTriple([eid(1), eid(2), eid(3)]);
        index
            .update(&[triple.clone()], &[], VersionNumber(1))
            .await
            .unwrap();

        // Confirm present
        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::ObjectId(eid(3)),
        ]);
        assert!(
            index
                .lookup(lookup, VersionNumber(1))
                .await
                .unwrap()
                .next()
                .is_some()
        );

        // Delete it
        index
            .update(&[], &[triple.clone()], VersionNumber(2))
            .await
            .unwrap();

        // Confirm gone
        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::ObjectId(eid(3)),
        ]);
        assert!(
            index
                .lookup(lookup, VersionNumber(2))
                .await
                .unwrap()
                .next()
                .is_none()
        );
    }

    fn create_encoding() -> ObjectIdEncoding {
        ObjectIdEncoding::new(4)
    }

    fn create_index() -> MemHashTripleIndex {
        create_index_with_batch_size(16)
    }

    fn create_index_with_batch_size(batch_size: usize) -> MemHashTripleIndex {
        let object_id_encoding = ObjectIdEncoding::new(4);
        let content = IndexContent {
            version: VersionNumber(0),
            index: Index::default(),
        };
        let index = MemHashTripleIndex {
            configuration: Arc::new(IndexConfiguration {
                batch_size,
                object_id_encoding,
                components: IndexComponents::try_new([
                    IndexComponent::Subject,
                    IndexComponent::Predicate,
                    IndexComponent::Object,
                ])
                .unwrap(),
            }),
            content: Arc::new(RwLock::new(content)),
        };
        index
    }

    fn eid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }

    fn oid_array(ids: &[u32]) -> ObjectIdArray {
        let array = ids.iter().copied().collect::<UInt32Array>();
        create_encoding().try_new_array(Arc::new(array)).unwrap()
    }

    async fn run_matching_test(
        index: MemHashTripleIndex,
        lookup: IndexLookup,
        expected_columns: usize,
        expected_rows: usize,
    ) {
        let results: Vec<_> = index
            .lookup(lookup, VersionNumber(1))
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
        lookup: IndexLookup,
        batch_size: usize,
        expected_batch_sizes: &[usize],
        ordered: bool,
    ) {
        let mut batch_sizes: Vec<_> = index
            .lookup(lookup, VersionNumber(1))
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
