use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::level_data::{IndexData, IndexDataScanState};
use crate::memory::storage::index::level_mapping::{
    IndexLevel, IndexLevelImpl, IndexLevelScanState,
};
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanError, IndexScanInstruction, IndexScanInstructions,
    IndexUpdateError, IndexedQuad, UnexpectedVersionNumberError,
};
use crate::memory::storage::VersionNumber;
use rdf_fusion_encoding::object_id::ObjectIdArray;
use std::sync::Arc;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Represents the index.
type IndexDataStructure = IndexLevel<IndexLevel<IndexLevel<IndexData>>>;

/// The full type of the index scan iterator.
type IndexScanState =
    IndexLevelScanState<IndexLevelScanState<IndexLevelScanState<IndexDataScanState>>>;

#[derive(Debug, Default)]
pub(super) struct IndexContent {
    /// The version that this index reflects.
    version: VersionNumber,
    /// The index.
    index: IndexDataStructure,
}

#[derive(Debug)]
pub struct MemHashTripleIndex {
    /// The index content.
    content: Arc<RwLock<IndexContent>>,
    /// The configuration of the index.
    configuration: IndexConfiguration,
}

impl MemHashTripleIndex {
    /// Creates a new [MemHashTripleIndex].
    pub fn new(configuration: IndexConfiguration) -> Self {
        let content = Arc::new(RwLock::new(IndexContent {
            version: VersionNumber(0),
            index: IndexDataStructure::default(),
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

    /// TODO
    pub async fn version_number(&self) -> VersionNumber {
        self.content.read().await.version
    }

    /// Returns the total number of quads.
    pub async fn len(
        &self,
        version_number: VersionNumber,
    ) -> Result<usize, IndexScanError> {
        let content = self.obtain_read_lock(version_number).await?;
        Ok(content.index.num_triples())
    }

    /// Performs a lookup in the index and returns a list of object arrays.
    ///
    /// See [MemHashIndexIterator] for more information.
    pub async fn scan(
        &self,
        lookup: IndexScanInstructions,
        version_number: VersionNumber,
    ) -> Result<MemHashIndexIterator, IndexScanError> {
        let content = self.obtain_read_lock(version_number).await?;
        Ok(MemHashIndexIterator::new(
            content,
            self.configuration.clone(),
            lookup,
        ))
    }

    /// Inserts a single entry at the top level, using the default levels for the inner. Can be used
    /// to insert named graphs without any triples.
    pub async fn scan_top_level(
        &self,
        version_number: VersionNumber,
    ) -> Result<Vec<EncodedObjectId>, IndexScanError> {
        let mut content = self.obtain_read_lock(version_number).await?;
        let result = content.index.scan_this_level();
        Ok(result)
    }

    /// Inserts a list of quads.
    ///
    /// Quads that already exist in the index are ignored.
    pub async fn insert(
        &self,
        quads: impl IntoIterator<Item = IndexedQuad>,
        version_number: VersionNumber,
    ) -> Result<usize, IndexUpdateError> {
        let mut content = self.obtain_write_lock(version_number).await?;

        let mut count = 0;
        for quad in quads {
            let was_inserted = content.index.insert(&self.configuration, &quad, 0);
            if was_inserted {
                count += 1;
            }
        }

        content.version = content.version.next();
        Ok(count)
    }

    /// Inserts a single entry at the top level, using the default levels for the inner. Can be used
    /// to insert named graphs without any triples.
    pub async fn insert_top_level(
        &self,
        object_id: EncodedObjectId,
        version_number: VersionNumber,
    ) -> Result<bool, IndexUpdateError> {
        let mut content = self.obtain_write_lock(version_number).await?;

        let result = content.index.insert_this_level(object_id);

        content.version = content.version.next();
        Ok(result)
    }

    /// TODO
    pub async fn clear_top_level(
        &self,
        object_id: EncodedObjectId,
        version_number: VersionNumber,
    ) -> Result<bool, IndexUpdateError> {
        let mut content = self.obtain_write_lock(version_number).await?;

        let result = content.index.clear_entry(object_id);

        content.version = content.version.next();
        Ok(result)
    }

    /// TODO
    pub async fn clear(
        &self,
        version_number: VersionNumber,
    ) -> Result<(), IndexUpdateError> {
        let mut content = self.obtain_write_lock(version_number).await?;
        content.index.clear_level();
        content.version = content.version.next();
        Ok(())
    }

    /// TODO
    pub async fn drop_top_level(
        &self,
        object_id: EncodedObjectId,
        version_number: VersionNumber,
    ) -> Result<bool, IndexUpdateError> {
        let mut content = self.obtain_write_lock(version_number).await?;
        let result = content.index.remove_this_level(object_id);
        content.version = content.version.next();
        Ok(result)
    }

    /// TODO
    pub async fn contains_top_level(
        &self,
        object_id: EncodedObjectId,
        version_number: VersionNumber,
    ) -> Result<bool, IndexUpdateError> {
        let mut content = self.obtain_read_lock(version_number).await.expect("TODO");

        let result = content.index.contains_this_level(object_id);

        Ok(result)
    }

    /// Removes a list of quads.
    ///
    /// Quads that do not exist in the index are ignored.
    pub async fn remove(
        &self,
        quads: impl IntoIterator<Item = IndexedQuad>,
        version_number: VersionNumber,
    ) -> Result<usize, IndexUpdateError> {
        let mut content = self.obtain_write_lock(version_number).await?;

        let mut count = 0;
        for quad in quads {
            let was_removed = content.index.remove(&self.configuration, &quad, 0);
            if was_removed {
                count += 1;
            }
        }

        content.version = content.version.next();
        Ok(count)
    }

    /// Deletes a list of quads.
    pub async fn delete_quads(&self, quads: impl Iterator<Item = &IndexedQuad>) {
        let mut content = self.content.write().await;
        for quad in quads {
            content.index.remove(&self.configuration, quad, 0);
        }
    }

    async fn obtain_read_lock(
        &self,
        version_number: VersionNumber,
    ) -> Result<OwnedRwLockReadGuard<IndexContent>, IndexScanError> {
        let mut content = self.content.clone().read_owned().await;
        if content.version != version_number {
            return Err(IndexScanError::UnexpectedVersionNumber(
                UnexpectedVersionNumberError(content.version, version_number),
            ));
        }
        Ok(content)
    }

    async fn obtain_write_lock(
        &self,
        version_number: VersionNumber,
    ) -> Result<OwnedRwLockWriteGuard<IndexContent>, IndexUpdateError> {
        let mut content = self.content.clone().write_owned().await;
        if content.version.next() != version_number {
            return Err(IndexUpdateError::UnexpectedVersionNumber(
                UnexpectedVersionNumberError(content.version.next(), version_number),
            ));
        }
        Ok(content)
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
pub struct MemHashIndexIterator {
    /// The iterator holds a read lock on the entire index such that another transaction cannot
    /// delete data from the index during iteration.
    index: OwnedRwLockReadGuard<IndexContent>,
    /// Additional context necessary for the iteration.
    configuration: IndexConfiguration,
    /// The states of the individual levels.
    state: Option<IndexScanState>,
}

impl MemHashIndexIterator {
    /// Creates a new [MemHashIndexIterator].
    fn new(
        index: OwnedRwLockReadGuard<IndexContent>,
        configuration: IndexConfiguration,
        lookup: IndexScanInstructions,
    ) -> Self {
        let state = build_state(lookup.0);
        Self {
            index,
            configuration,
            state: Some(state),
        }
    }
}

impl Iterator for MemHashIndexIterator {
    type Item = Vec<ObjectIdArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let state = self.state.take()?;
        let result = self.index.index.scan(&self.configuration, state);
        self.state = result.new_state;
        result.result
    }
}

fn build_state(patterns: [IndexScanInstruction; 4]) -> IndexScanState {
    let data = create_state_for_data(patterns[3].clone());
    let first_level = create_state_for_level(patterns[2].clone(), data);
    let second_level = create_state_for_level(patterns[1].clone(), first_level);
    create_state_for_level(patterns[0].clone(), second_level)
}

fn create_state_for_data(scan_instruction: IndexScanInstruction) -> IndexDataScanState {
    match scan_instruction {
        IndexScanInstruction::Traverse(predicate) => {
            IndexDataScanState::Traverse { predicate }
        }
        IndexScanInstruction::Scan(predicate) => IndexDataScanState::Scan {
            predicate,
            consumed: 0,
        },
    }
}

pub fn create_state_for_level<TInner: Clone>(
    scan_instruction: IndexScanInstruction,
    inner: TInner,
) -> IndexLevelScanState<TInner> {
    match scan_instruction {
        IndexScanInstruction::Traverse(predicate) => {
            IndexLevelScanState::traverse(predicate, inner)
        }
        IndexScanInstruction::Scan(predicate) => {
            IndexLevelScanState::scan(predicate, inner)
        }
    }
}
