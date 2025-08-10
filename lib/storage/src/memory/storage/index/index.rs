use crate::memory::encoding::EncodedObjectIdPattern;
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::index::error::{IndexDeletionError, IndexUpdateError};
use crate::memory::storage::index::level::{
    IndexIterationContext, IndexLevel, IndexLevelActionResult, IndexLevelActionState,
    IndexLevelImpl,
};
use crate::memory::storage::VersionNumber;
use datafusion::arrow::array::{Array, UInt32Array};
use datafusion::common::exec_err;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::{ObjectIdArray, ObjectIdEncoding};
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use std::collections::HashSet;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

/// Represents the index.
type Index = IndexLevel<IndexLevel<IndexData>>;

/// Holds the data for the last index level.
struct IndexData {
    /// The object ids. The index tries to keep the object ids in batches of `batch_size`.
    arrays: Vec<ObjectIdArray>,
    /// The currently building object ids.
    building: Vec<EncodedObjectId>,
}

/// Represents what part of an RDF triple is index at the given position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexedComponent {
    /// The subject
    Subject,
    /// The predicate
    Predicate,
    /// The object
    Object,
}

/// Represents a list of *disjunct* index components.
pub struct IndexConfiguration([IndexedComponent; 3]);

#[derive(Debug, Error)]
#[error("Duplicate indexed component given.")]
struct IndexedComponentsCreationError;

impl IndexConfiguration {
    /// Tries to create a new [IndexConfiguration].
    ///
    /// Returns an error if an [IndexComponent] appears more than once.
    pub fn try_new(
        components: [IndexedComponent; 3],
    ) -> Result<Self, IndexedComponentsCreationError> {
        let distinct = components.iter().collect::<HashSet<_>>();
        if distinct.len() != 3 {
            return Err(IndexedComponentsCreationError);
        }

        Ok(IndexConfiguration(components))
    }
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
    /// Differentiates between multiple configurations (e.g., SPO, PSO).
    configuration: IndexedComponent,
    /// The encoding used for object ids.
    object_id_encoding: ObjectIdEncoding,
}

impl MemHashTripleIndex {
    /// Performs a lookup in the index and returns a list of object arrays.
    ///
    /// See [MemHashTripleIndexIterator] for more information.
    pub async fn lookup(
        &self,
        lookup: IndexLookup,
        version_number: VersionNumber,
        batch_size: usize,
    ) -> DFResult<MemHashTripleIndexIterator> {
        let lock = self.content.clone().read_owned().await;
        if lock.version > version_number {
            return exec_err!("Index is already past the inquired version.");
        }
        Ok(MemHashTripleIndexIterator::new(
            lock,
            self.object_id_encoding.clone(),
            lookup,
            batch_size,
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
            index.index.insert_triple(triple, 0, 0);
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
    context: IndexIterationContext,
    /// The states of the individual levels.
    state: Option<Box<IndexLevelActionState>>,
}

impl MemHashTripleIndexIterator {
    /// Creates a new [MemHashTripleIndexIterator].
    pub fn new(
        index: OwnedRwLockReadGuard<IndexContent>,
        object_id_encoding: ObjectIdEncoding,
        lookup: IndexLookup,
        batch_size: usize,
    ) -> Self {
        let state = build_state(lookup.0.as_slice());
        let context = IndexIterationContext {
            object_id_encoding,
            batch_size,
        };
        Self {
            index,
            context,
            state: Some(Box::new(state)),
        }
    }
}

fn build_state(patterns: &[EncodedObjectIdPattern]) -> IndexLevelActionState {
    let inner = match patterns.len() {
        1 => None,
        _ => Some(Box::new(build_state(&patterns[1..]))),
    };

    match patterns.first().expect("Recursion ends at len=1") {
        EncodedObjectIdPattern::ObjectId(object_id) => IndexLevelActionState::Lookup {
            object_id: *object_id,
            inner,
        },
        EncodedObjectIdPattern::Variable => IndexLevelActionState::Scan {
            default_state: inner.clone(),
            consumed: 0,
            inner,
        },
    }
}

impl Iterator for MemHashTripleIndexIterator {
    type Item = Vec<ObjectIdArray>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(state) = self.state.take() else {
            return None;
        };

        let result = self.index.index.execute_scan_action(&self.context, state);
        self.state = result.new_state;
        result.result
    }
}

impl IndexLevelImpl for IndexData {
    fn create_empty() -> Self {
        Self {
            arrays: Vec::new(),
            building: Vec::new(),
        }
    }

    fn insert_triple(
        &mut self,
        triple: &IndexedTriple,
        cur_depth: usize,
        batch_size: usize,
    ) {
        let part = triple.0[cur_depth];
        self.building.push(part);

        if self.building.len() == batch_size {
            todo!("append")
        }
    }

    fn delete_triple(
        &mut self,
        triple: &IndexedTriple,
        cur_depth: usize,
    ) -> Result<(), IndexDeletionError> {
        todo!()
    }

    fn num_triples(&self) -> usize {
        self.arrays.iter().map(|a| a.array().len()).sum()
    }

    fn lookup(
        &self,
        _context: &IndexIterationContext,
        _inner: Option<Box<IndexLevelActionState>>,
        oid: EncodedObjectId,
    ) -> IndexLevelActionResult {
        for array in &self.arrays {
            let contains = array
                .object_ids()
                .values()
                .iter()
                .any(|arr_id| EncodedObjectId::from(*arr_id) == oid);
            if contains {
                return IndexLevelActionResult::finished(1, Some(vec![]));
            }
        }

        let contains = self.building.iter().any(|arr_id| *arr_id == oid);
        if contains {
            IndexLevelActionResult::finished(1, Some(vec![]))
        } else {
            IndexLevelActionResult::empty_finished()
        }
    }

    fn scan(
        &self,
        context: &IndexIterationContext,
        _inner: Option<Box<IndexLevelActionState>>,
        _default_state: Option<Box<IndexLevelActionState>>,
        consumed: usize,
    ) -> IndexLevelActionResult {
        if consumed < self.arrays.len() {
            return IndexLevelActionResult {
                num_results: self.arrays[consumed].object_ids().len(),
                result: Some(vec![self.arrays[consumed].clone()]),
                new_state: Some(Box::new(IndexLevelActionState::Scan {
                    consumed: consumed + 1,
                    default_state: None,
                    inner: None,
                })),
            };
        }

        if consumed == self.arrays.len() {
            let iterator = self.building.iter().map(|id| id.as_u32());
            let uint_array = UInt32Array::from_iter_values(iterator);

            let array = context
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::UInt32Array;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use rdf_fusion_encoding::TermEncoding;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[test]
    fn index_configuration_accepts_unique_components() {
        let ok = IndexConfiguration::try_new([
            IndexedComponent::Subject,
            IndexedComponent::Predicate,
            IndexedComponent::Object,
        ]);
        assert!(ok.is_ok());
    }

    #[test]
    fn index_configuration_rejects_duplicate_components() {
        let err = IndexConfiguration::try_new([
            IndexedComponent::Subject,
            IndexedComponent::Subject,
            IndexedComponent::Object,
        ]);
        assert!(err.is_err());
    }

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
        let mut iter = index.lookup(lookup, VersionNumber(1), 10).await.unwrap();
        let result = iter.next();

        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 0); // Nothing is bound to a variable -> No outputs.
    }

    #[tokio::test]
    async fn insert_and_lookup_subject_var() {
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
        let results: Vec<_> = index
            .lookup(lookup, VersionNumber(1), 10)
            .await
            .unwrap()
            .collect();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].len(), 2);
        assert_eq!(results[1].len(), 2);
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
                .lookup(lookup, VersionNumber(1), 10)
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
                .lookup(lookup, VersionNumber(2), 10)
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
        let object_id_encoding = ObjectIdEncoding::new(4);
        let content = IndexContent {
            version: VersionNumber(0),
            index: Index::default(),
        };
        let index = MemHashTripleIndex {
            object_id_encoding,
            content: Arc::new(RwLock::new(content)),
            configuration: IndexedComponent::Subject,
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
}
