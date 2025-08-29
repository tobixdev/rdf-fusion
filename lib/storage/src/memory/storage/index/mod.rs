use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};

mod components;
mod error;
mod hash_index;
mod level;
mod level_data;
mod level_mapping;
mod scan;
mod scan_collector;
mod set;

use crate::memory::encoding::{EncodedActiveGraph, EncodedTermPattern};
use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedObjectId};
pub use components::IndexComponents;
pub use error::*;
use rdf_fusion_model::Variable;
pub use scan::{IndexScanBatch, MemHashIndexIterator, PreparedIndexScan};
pub use set::IndexSet;

#[derive(Debug, Clone)]
pub struct IndexedQuad(pub [EncodedObjectId; 4]);

/// Holds the configuration for the index.
#[derive(Debug, Clone)]
pub struct IndexConfiguration {
    /// The object id encoding.
    pub object_id_encoding: ObjectIdEncoding,
    /// The desired batch size. This iterator only provides a best-effort service for adhering to
    /// the batch size.
    pub batch_size: usize,
    /// Differentiates between multiple configurations (e.g., SPO, PSO).
    pub components: IndexComponents,
}

impl Display for IndexConfiguration {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.components)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexScanInstructions(pub [IndexScanInstruction; 4]);

/// A predicate for filtering object ids.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ObjectIdScanPredicate {
    /// Checks whether the object id is in the given set.
    In(HashSet<EncodedObjectId>),
    /// Checks whether the object id is *not* in the given set.
    Except(HashSet<EncodedObjectId>),
}

impl ObjectIdScanPredicate {
    /// Indicates whether the predicate restricts to a set of known size.
    ///
    /// For example, [ObjectIdScanPredicate::In] restricts to a known size, while
    /// [ObjectIdScanPredicate::Except] does not, as the universe of object ids is not known.
    pub fn restricts_to_known_size(&self) -> bool {
        match self {
            ObjectIdScanPredicate::In(_) => true,
            ObjectIdScanPredicate::Except(_) => false,
        }
    }

    /// Evaluates the predicate for the given object id.
    pub fn evaluate(&self, object_id: EncodedObjectId) -> bool {
        match self {
            ObjectIdScanPredicate::In(ids) => ids.contains(&object_id),
            ObjectIdScanPredicate::Except(ids) => !ids.contains(&object_id),
        }
    }
}

/// An encoded version of a triple pattern.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IndexScanInstruction {
    /// Traverses the index level, not binding the elements at this level.
    Traverse(Option<ObjectIdScanPredicate>),
    /// Scans the index level, binding the elements at this level.
    Scan(String, Option<ObjectIdScanPredicate>),
}

impl IndexScanInstruction {
    pub fn scan_variable(&self) -> Option<&str> {
        match self {
            IndexScanInstruction::Traverse(_) => None,
            IndexScanInstruction::Scan(variable, _) => Some(variable.as_str()),
        }
    }

    pub fn predicate(&self) -> Option<&ObjectIdScanPredicate> {
        match self {
            IndexScanInstruction::Traverse(predicate) => predicate.as_ref(),
            IndexScanInstruction::Scan(_, predicate) => predicate.as_ref(),
        }
    }
}

impl IndexScanInstruction {
    /// Returns the [IndexScanInstruction] for reading the given [EncodedActiveGraph], also
    /// considering whether the graph name is bound to a `variable`.
    pub fn from_active_graph(
        active_graph: &EncodedActiveGraph,
        variable: Option<&Variable>,
    ) -> IndexScanInstruction {
        let instruction_with_predicate = |predicate: Option<ObjectIdScanPredicate>| {
            if let Some(variable) = variable {
                IndexScanInstruction::Scan(variable.as_str().to_owned(), predicate)
            } else {
                IndexScanInstruction::Traverse(predicate)
            }
        };

        match active_graph {
            EncodedActiveGraph::DefaultGraph => {
                let object_ids = HashSet::from([DEFAULT_GRAPH_ID.0]);
                instruction_with_predicate(Some(ObjectIdScanPredicate::In(object_ids)))
            }
            EncodedActiveGraph::AllGraphs => instruction_with_predicate(None),
            EncodedActiveGraph::Union(graphs) => {
                let object_ids = HashSet::from_iter(graphs.iter().map(|g| g.0));
                instruction_with_predicate(Some(ObjectIdScanPredicate::In(object_ids)))
            }
            EncodedActiveGraph::AnyNamedGraph => {
                let object_ids = HashSet::from([DEFAULT_GRAPH_ID.0]);
                instruction_with_predicate(Some(ObjectIdScanPredicate::Except(
                    object_ids,
                )))
            }
        }
    }
}

impl From<EncodedTermPattern> for IndexScanInstruction {
    fn from(value: EncodedTermPattern) -> Self {
        match value {
            EncodedTermPattern::ObjectId(object_id) => IndexScanInstruction::Traverse(
                Some(ObjectIdScanPredicate::In(HashSet::from([object_id]))),
            ),
            EncodedTermPattern::Variable(var) => IndexScanInstruction::Scan(var, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::storage::VersionNumber;
    use crate::memory::storage::index::components::IndexComponent;
    use crate::memory::storage::index::hash_index::MemHashTripleIndex;
    use crate::memory::storage::index::{
        IndexComponents, IndexScanError, IndexScanInstruction, IndexScanInstructions,
        UnexpectedVersionNumberError,
    };
    use datafusion::arrow::array::Array;
    use insta::assert_debug_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;

    #[tokio::test]
    async fn insert_and_scan_triple() {
        let index = create_index();
        let quads = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index.insert(quads, VersionNumber(1)).await.unwrap();

        let mut iter = index
            .create_scan(
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
        assert_eq!(result.unwrap().num_results, 1);
    }

    #[tokio::test]
    async fn scan_returns_sorted_results_on_last_level() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(2), eid(2)]),
        ];
        index.insert(quads, VersionNumber(1)).await.unwrap();

        let mut iter = index
            .create_scan(
                IndexScanInstructions([traverse(0), traverse(1), traverse(2), scan("d")]),
                VersionNumber(1),
            )
            .await
            .unwrap();
        let result = iter.next();

        assert!(result.is_some());
        assert_debug_snapshot!(result.unwrap().columns.get("d").unwrap().object_ids(), @r"
        PrimitiveArray<UInt32>
        [
          2,
          3,
        ]
        ");
    }

    #[tokio::test]
    async fn scan_returns_sorted_results_on_intermediate_level() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(1), eid(3)]),
        ];
        index.insert(quads, VersionNumber(1)).await.unwrap();

        let mut iter = index
            .create_scan(
                IndexScanInstructions([traverse(0), traverse(1), scan("c"), traverse(3)]),
                VersionNumber(1),
            )
            .await
            .unwrap();
        let result = iter.next();

        assert!(result.is_some());
        assert_debug_snapshot!(result.unwrap().columns.get("c").unwrap().object_ids(), @r"
        PrimitiveArray<UInt32>
        [
          1,
          2,
        ]
        ");
    }

    #[tokio::test]
    async fn scan_newer_index_version_err() {
        let index = create_index();
        let quads = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index.insert(quads.clone(), VersionNumber(1)).await.unwrap();
        index.remove(quads, VersionNumber(2)).await.unwrap();

        assert_eq!(
            index
                .create_scan(
                    IndexScanInstructions([scan("a"), scan("b"), scan("c"), scan("d")]),
                    VersionNumber(1)
                )
                .await
                .err(),
            Some(IndexScanError::UnexpectedVersionNumber(
                UnexpectedVersionNumberError(VersionNumber(2), VersionNumber(1))
            ))
        );
    }

    #[tokio::test]
    async fn scan_with_no_match() {
        let index = create_index();
        let quads = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        index.insert(quads, VersionNumber(1)).await.unwrap();

        let version_number = index.version_number().await;
        let result = index
            .create_scan(
                IndexScanInstructions([traverse(1), scan("b"), traverse(2), traverse(3)]),
                version_number,
            )
            .await
            .unwrap()
            .next();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn scan_subject_var() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(0), eid(6), eid(2), eid(3)]),
        ];
        index.insert(quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), scan("b"), traverse(2), traverse(3)]),
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
        index.insert(quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), scan("c"), traverse(3)]),
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
        index.insert(quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), traverse(2), scan("d")]),
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
        index.insert(quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([traverse(0), scan("b"), traverse(2), scan("d")]),
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
        index.insert(quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([scan("a"), scan("b"), scan("c"), scan("d")]),
            4,
            3,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_same_var_appearing_twice() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(2), eid(2), eid(3)]),
            IndexedQuad([eid(0), eid(1), eid(1), eid(3)]),
            IndexedQuad([eid(0), eid(2), eid(1), eid(3)]),
        ];
        index.insert(quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([scan("a"), scan("same"), scan("same"), scan("d")]),
            3,
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_considers_predicates() {
        let index = create_index();
        let quads = vec![
            IndexedQuad([eid(0), eid(1), eid(2), eid(3)]),
            IndexedQuad([eid(1), eid(1), eid(4), eid(5)]),
            IndexedQuad([eid(2), eid(6), eid(2), eid(3)]),
        ];
        index.insert(quads, VersionNumber(1)).await.unwrap();

        run_matching_test(
            index,
            IndexScanInstructions([
                IndexScanInstruction::Scan(
                    "a".to_owned(),
                    Some(ObjectIdScanPredicate::In(HashSet::from([eid(0), eid(2)]))),
                ),
                scan("b"),
                scan("c"),
                scan("d"),
            ]),
            4,
            2,
        )
        .await;
    }

    #[tokio::test]
    async fn scan_batches_for_batch_size() {
        let index = create_index_with_batch_size(10);
        let mut quads = Vec::new();
        for i in 0..25 {
            quads.push(IndexedQuad([eid(0), eid(1), eid(2), eid(i + 1)]))
        }
        index.insert(quads, VersionNumber(1)).await.unwrap();

        // The lookup matches a single IndexData that will be scanned.
        run_batch_size_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), traverse(2), scan("d")]),
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
        index.insert(quads, VersionNumber(1)).await.unwrap();

        // The lookup matches 25 different IndexLevels, each having exactly one data entry. The
        // batches should be combined into a single batch.
        run_batch_size_test(
            index,
            IndexScanInstructions([traverse(0), traverse(1), scan("c"), traverse(2)]),
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
        index.insert(quads.clone(), VersionNumber(1)).await.unwrap();
        index.remove(quads, VersionNumber(2)).await.unwrap();

        run_non_matching_test(
            index,
            IndexScanInstructions([scan("a"), scan("b"), scan("c"), scan("d")]),
        )
        .await;
    }

    #[tokio::test]
    async fn delete_triple_non_existing_ok() {
        let index = create_index();
        let quads = vec![IndexedQuad([eid(0), eid(1), eid(2), eid(3)])];
        let result = index.remove(quads, VersionNumber(1)).await;
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

    fn scan(name: impl Into<String>) -> IndexScanInstruction {
        IndexScanInstruction::Scan(name.into(), None)
    }

    fn eid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }

    async fn run_non_matching_test(
        index: MemHashTripleIndex,
        lookup: IndexScanInstructions,
    ) {
        let version_number = index.version_number().await;
        let results = index
            .create_scan(lookup, version_number)
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
        let version_number = index.version_number().await;
        let results = index
            .create_scan(lookup, version_number)
            .await
            .unwrap()
            .next()
            .unwrap();

        assert_eq!(results.num_results, expected_rows);
        assert_eq!(results.columns.len(), expected_columns);
        for result in results.columns.values() {
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
            .create_scan(lookup, VersionNumber(1))
            .await
            .unwrap()
            .map(|arr| arr.num_results)
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
