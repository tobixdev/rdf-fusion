use crate::memory::encoding::EncodedObjectIdPattern;
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::VersionNumber;
use crate::memory::storage::index::IndexConfiguration;
use crate::memory::storage::index::error::{
    IndexDeletionError, IndexScanError, IndexUpdateError,
};
use crate::memory::storage::index::index_level::{
    IndexLevel, IndexLevelActionResult, IndexLevelImpl, IndexLevelScanState,
    create_state_for_level,
};
use datafusion::arrow::array::{Array, UInt32Array};
use rdf_fusion_encoding::object_id::ObjectIdArray;
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// Represents the index.
type Index = IndexLevel<IndexLevel<IndexLevel<IndexData>>>;

/// The full type of the index scan iterator.
type IndexScanState =
    IndexLevelScanState<IndexLevelScanState<IndexLevelScanState<IndexDataScanState>>>;

/// Holds the data for the last index level.
struct IndexData {
    /// The object ids. The index tries to keep the object ids in batches of `batch_size`.
    arrays: Vec<ObjectIdArray>,
    /// The currently building object ids.
    building: Vec<EncodedObjectId>,
}

#[derive(Debug, Clone)]
pub struct IndexLookup(pub [EncodedObjectIdPattern; 4]);

#[derive(Debug, Clone)]
pub struct IndexedQuad(pub [EncodedObjectId; 4]);

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
    configuration: IndexConfiguration,
}

impl MemHashTripleIndex {
    /// Creates a new [MemHashTripleIndex].
    pub fn new(configuration: IndexConfiguration) -> Self {
        let index = Index::create_empty();
        let content = Arc::new(RwLock::new(IndexContent {
            version: VersionNumber(0),
            index,
        }));
        Self {
            content,
            configuration,
        }
    }

    /// Returns a reference to the index configuration.
    pub fn configuration(&self) -> &IndexConfiguration {
        &self.configuration
    }

    /// Performs a lookup in the index and returns a list of object arrays.
    ///
    /// See [MemHashTripleIndexIterator] for more information.
    pub async fn scan(
        &self,
        lookup: IndexLookup,
        version_number: VersionNumber,
    ) -> Result<MemHashTripleIndexIterator, IndexScanError> {
        let lock = self.content.clone().read_owned().await;
        if lock.version > version_number {
            return Err(IndexScanError::UnexpectedIndexVersionNumber);
        }
        Ok(MemHashTripleIndexIterator::new(
            lock,
            self.configuration.clone(),
            lookup,
        ))
    }

    pub async fn update(
        &self,
        to_insert: &[IndexedQuad],
        to_delete: &[IndexedQuad],
        version_number: VersionNumber,
    ) -> Result<(), IndexUpdateError> {
        let mut index = self.content.write().await;

        for triple in to_insert {
            index.index.insert_triple(&self.configuration, triple, 0);
        }

        for triple in to_delete {
            index.index.delete_triple(&self.configuration, triple, 0)?;
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
    configuration: IndexConfiguration,
    /// The states of the individual levels.
    state: Option<IndexScanState>,
}

impl MemHashTripleIndexIterator {
    /// Creates a new [MemHashTripleIndexIterator].
    fn new(
        index: OwnedRwLockReadGuard<IndexContent>,
        configuration: IndexConfiguration,
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

fn build_state(patterns: [EncodedObjectIdPattern; 4]) -> IndexScanState {
    let data = create_state_for_data(patterns[3]);
    let first_level = create_state_for_level(patterns[2], data);
    let second_level = create_state_for_level(patterns[1], first_level);
    create_state_for_level(patterns[0], second_level)
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
        let state = self.state.take()?;
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

        let contains = self.building.contains(&object_id);
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
        triple: &IndexedQuad,
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
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> Result<(), IndexDeletionError> {
        let part = triple.0[cur_depth];

        let building_idx = self.building.iter().position(|id| *id == part);
        if let Some(building_idx) = building_idx {
            self.building.swap_remove(building_idx);
            return Ok(());
        }

        for array in &mut self.arrays {
            let iterator = array
                .object_ids()
                .values()
                .iter()
                .copied()
                .filter(|id| *id != part.as_u32());
            let new_array = UInt32Array::from_iter_values(iterator);

            if new_array.len() < array.object_ids().len() {
                *array = configuration
                    .object_id_encoding
                    .try_new_array(Arc::new(new_array))
                    .expect("TODO ObjectIdArray");
                return Ok(());
            }
        }

        Err(IndexDeletionError::NonExistingTriple)
    }

    fn num_triples(&self) -> usize {
        self.arrays.iter().map(|a| a.array().len()).sum::<usize>() + self.building.len()
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
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn insert_and_scan_triple() {
        let index = create_index();
        let triples = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(0)),
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::ObjectId(eid(3)),
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();
        index.update(&[], &triples, VersionNumber(2)).await.unwrap();

        let result = index
            .scan(
                IndexLookup([EncodedObjectIdPattern::Variable; 4]),
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(0)),
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::ObjectId(eid(3)),
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(0)),
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(3)),
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(0)),
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::Variable,
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(0)),
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::Variable,
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        let lookup = IndexLookup([EncodedObjectIdPattern::Variable; 4]);

        run_matching_test(index, lookup, 4, 3).await;
    }

    #[tokio::test]
    async fn scan_batches_for_batch_size() {
        let index = create_index_with_batch_size(10);
        let mut triples = Vec::new();
        for i in 0..25 {
            triples.push(IndexedQuad([eid(0), eid(1), eid(2), eid(i)]))
        }
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        // The lookup matches a single IndexData that will be scanned.
        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(0)),
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::ObjectId(eid(2)),
            EncodedObjectIdPattern::Variable,
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();

        // The lookup matches 25 different IndexLevels, each having exactly one data entry. The
        // batches should be combined into a single batch.
        let lookup = IndexLookup([
            EncodedObjectIdPattern::ObjectId(eid(0)),
            EncodedObjectIdPattern::ObjectId(eid(1)),
            EncodedObjectIdPattern::Variable,
            EncodedObjectIdPattern::ObjectId(eid(2)),
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
        index.update(&triples, &[], VersionNumber(1)).await.unwrap();
        index.update(&[], &triples, VersionNumber(2)).await.unwrap();

        run_non_matching_test(index, IndexLookup([EncodedObjectIdPattern::Variable; 4]))
            .await;
    }

    #[tokio::test]
    async fn delete_triple_non_existing_err() {
        let index = create_index();
        let triples = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        let result = index.update(&[], &triples, VersionNumber(1)).await;
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
        let content = IndexContent {
            version: VersionNumber(0),
            index: Index::default(),
        };
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

    async fn run_non_matching_test(index: MemHashTripleIndex, lookup: IndexLookup) {
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
        lookup: IndexLookup,
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
        lookup: IndexLookup,
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
