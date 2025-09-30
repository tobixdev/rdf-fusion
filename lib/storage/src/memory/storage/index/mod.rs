use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

mod components;
mod error;
mod quad_index;
mod quad_index_data;
mod scan;
mod set;

use crate::memory::encoding::{EncodedActiveGraph, EncodedTermPattern};
use crate::memory::object_id::{EncodedObjectId, DEFAULT_GRAPH_ID};
pub use components::IndexComponents;
pub use error::*;
use rdf_fusion_model::Variable;
pub use scan::{
    DirectIndexRef, IndexRefInSet, MemQuadIndexScanIterator, PlannedPatternScan,
};
pub use set::{IndexSet, MemQuadIndexSetScanIterator};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexedQuad(pub [EncodedObjectId; 4]);

/// Holds the configuration for the index.
#[derive(Debug, Clone, PartialEq, Eq)]
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
pub struct IndexScanInstructions([IndexScanInstruction; 4]);

impl IndexScanInstructions {
    /// Creates a new [IndexScanInstructions] from the given [IndexScanInstruction]s.
    ///
    /// If more than one scans bind to a given variable, equality checks are handled automatically.
    pub fn new(instructions: [IndexScanInstruction; 4]) -> Self {
        let mut new_instructions = Vec::new();
        let mut seen = HashSet::new();

        for instruction in instructions {
            match instruction {
                IndexScanInstruction::Scan(var, predicate) => {
                    let inserted = seen.insert(Arc::clone(&var));
                    if inserted {
                        new_instructions.push(IndexScanInstruction::Scan(var, predicate))
                    } else {
                        new_instructions.push(IndexScanInstruction::Traverse(Some(
                            ObjectIdScanPredicate::EqualTo(Arc::clone(&var)),
                        )));
                    }
                }
                instruction => {
                    new_instructions.push(instruction);
                }
            }
        }

        Self(new_instructions.try_into().unwrap())
    }

    pub fn instructions(&self) -> &[IndexScanInstruction; 4] {
        &self.0
    }
}

/// A predicate for filtering object ids.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ObjectIdScanPredicate {
    /// Checks whether the object id is in the given set.
    In(BTreeSet<EncodedObjectId>),
    /// Checks whether the object id is *not* in the given set.
    Except(BTreeSet<EncodedObjectId>),
    /// Checks whether the object id is equal to the scan instruction with the given variable.
    EqualTo(Arc<String>),
    /// Checks whether the object id is between the given object ids (end is inclusive).
    Between(EncodedObjectId, EncodedObjectId),
}

/// An encoded version of a triple pattern.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IndexScanInstruction {
    /// Traverses the index level, not binding the elements at this level.
    Traverse(Option<ObjectIdScanPredicate>),
    /// Scans the index level, binding the elements at this level.
    Scan(Arc<String>, Option<ObjectIdScanPredicate>),
}

impl IndexScanInstruction {
    /// Returns the scan variable (i.e., the variable to bind the results to) for this instruction.
    pub fn scan_variable(&self) -> Option<&str> {
        match self {
            IndexScanInstruction::Traverse(_) => None,
            IndexScanInstruction::Scan(variable, _) => Some(variable.as_str()),
        }
    }

    /// Returns the predicate for this instruction.
    pub fn predicate(&self) -> Option<&ObjectIdScanPredicate> {
        match self {
            IndexScanInstruction::Traverse(predicate) => predicate.as_ref(),
            IndexScanInstruction::Scan(_, predicate) => predicate.as_ref(),
        }
    }

    /// Creates a new [IndexScanInstruction] that has no predicate, even if the original instruction
    /// contained a predicate.
    pub fn without_predicate(&self) -> Self {
        match self {
            IndexScanInstruction::Traverse(_) => IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Scan(variable, _) => {
                IndexScanInstruction::Scan(Arc::clone(variable), None)
            }
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
                IndexScanInstruction::Scan(
                    Arc::new(variable.as_str().to_owned()),
                    predicate,
                )
            } else {
                IndexScanInstruction::Traverse(predicate)
            }
        };

        match active_graph {
            EncodedActiveGraph::DefaultGraph => {
                let object_ids = BTreeSet::from([DEFAULT_GRAPH_ID.0]);
                instruction_with_predicate(Some(ObjectIdScanPredicate::In(object_ids)))
            }
            EncodedActiveGraph::AllGraphs => instruction_with_predicate(None),
            EncodedActiveGraph::Union(graphs) => {
                let object_ids = BTreeSet::from_iter(graphs.iter().map(|g| g.0));
                instruction_with_predicate(Some(ObjectIdScanPredicate::In(object_ids)))
            }
            EncodedActiveGraph::AnyNamedGraph => {
                let object_ids = BTreeSet::from([DEFAULT_GRAPH_ID.0]);
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
                Some(ObjectIdScanPredicate::In(BTreeSet::from([object_id]))),
            ),
            EncodedTermPattern::Variable(var) => {
                IndexScanInstruction::Scan(Arc::new(var), None)
            }
        }
    }
}

/// TODO
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PruningPredicates([Option<PruningPredicate>; 4]);

impl From<&IndexScanInstructions> for PruningPredicates {
    fn from(value: &IndexScanInstructions) -> Self {
        let predicates = value
            .0
            .iter()
            .map(|i| i.predicate().and_then(Option::<PruningPredicate>::from))
            .collect::<Vec<_>>();
        Self(predicates.try_into().expect("Should yield 4 predicates"))
    }
}

/// TODO
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum PruningPredicate {
    /// Checks whether the object id is in the given set.
    EqualTo(EncodedObjectId),
    /// Checks whether the object id is between the given object ids (end is inclusive).
    Between(EncodedObjectId, EncodedObjectId),
}

impl From<&ObjectIdScanPredicate> for Option<PruningPredicate> {
    fn from(value: &ObjectIdScanPredicate) -> Self {
        match value {
            ObjectIdScanPredicate::In(ids) => {
                let predicate = if ids.len() == 1 {
                    PruningPredicate::EqualTo(*ids.first().unwrap())
                } else {
                    PruningPredicate::Between(*ids.first().unwrap(), *ids.last().unwrap())
                };
                Some(predicate)
            }
            ObjectIdScanPredicate::Between(from, to) => {
                Some(PruningPredicate::Between(*from, *to))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::storage::index::components::IndexComponent;
    use crate::memory::storage::index::quad_index::MemQuadIndex;
    use crate::memory::storage::index::{
        IndexComponents, IndexScanInstruction, IndexScanInstructions,
    };
    use datafusion::arrow::array::Array;
    use insta::assert_debug_snapshot;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;

    #[tokio::test]
    async fn insert_and_scan_triple() {
        let mut index = create_index();
        index.insert(vec![IndexedQuad([eid(1), eid(2), eid(3), eid(4)])]);

        let mut iter = index.scan_quads(IndexScanInstructions::new([
            traverse(1),
            traverse(2),
            traverse(3),
            traverse(4),
        ]));
        let result = iter.next();

        assert!(result.is_some());
        assert_eq!(result.unwrap().num_rows, 1);
    }

    #[tokio::test]
    async fn scan_returns_sorted_results_on_last_level() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(3), eid(3)]),
        ]);

        let mut iter = index.scan_quads(IndexScanInstructions::new([
            traverse(1),
            traverse(2),
            traverse(3),
            scan("d"),
        ]));
        let result = iter.next();

        assert!(result.is_some());
        assert_debug_snapshot!(result.unwrap().columns, @r#"
        {
            "d": PrimitiveArray<UInt32>
            [
              3,
              4,
            ],
        }
        "#);
    }

    #[tokio::test]
    async fn scan_returns_sorted_results_on_intermediate_level() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(2), eid(4)]),
        ]);

        let mut iter = index.scan_quads(IndexScanInstructions::new([
            traverse(1),
            traverse(2),
            scan("c"),
            traverse(4),
        ]));
        let result = iter.next();

        assert!(result.is_some());
        assert_debug_snapshot!(result.unwrap().columns, @r#"
        {
            "c": PrimitiveArray<UInt32>
            [
              2,
              3,
            ],
        }
        "#);
    }

    #[tokio::test]
    async fn scan_with_no_match() {
        let mut index = create_index();
        index.insert(vec![IndexedQuad([eid(1), eid(2), eid(3), eid(4)])]);

        let result = index
            .scan_quads(IndexScanInstructions::new([
                traverse(2),
                scan("b"),
                traverse(3),
                traverse(4),
            ]))
            .next();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn scan_subject_var() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexedQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            IndexScanInstructions::new([
                traverse(1),
                scan("b"),
                traverse(3),
                traverse(4),
            ]),
            1,
            2,
        );
    }

    #[tokio::test]
    async fn scan_predicate_var() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexedQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            IndexScanInstructions::new([
                traverse(1),
                traverse(2),
                scan("c"),
                traverse(4),
            ]),
            1,
            1,
        );
    }

    #[tokio::test]
    async fn scan_object_var() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexedQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            IndexScanInstructions::new([
                traverse(1),
                traverse(2),
                traverse(3),
                scan("d"),
            ]),
            1,
            1,
        );
    }

    #[tokio::test]
    async fn scan_multi_vars() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexedQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            IndexScanInstructions::new([traverse(1), scan("b"), traverse(3), scan("d")]),
            2,
            2,
        );
    }

    #[tokio::test]
    async fn scan_all_vars() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexedQuad([eid(1), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            IndexScanInstructions::new([scan("a"), scan("b"), scan("c"), scan("d")]),
            4,
            3,
        );
    }

    #[tokio::test]
    async fn scan_same_var_appearing_twice() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(3), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(2), eid(4)]),
            IndexedQuad([eid(1), eid(3), eid(2), eid(4)]),
        ]);

        run_matching_test(
            index,
            IndexScanInstructions::new([
                scan("a"),
                scan("same"),
                scan("same"),
                scan("d"),
            ]),
            3,
            2,
        );
    }

    #[tokio::test]
    async fn scan_considers_predicates() {
        let mut index = create_index();
        index.insert(vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(2), eid(2), eid(5), eid(6)]),
            IndexedQuad([eid(3), eid(7), eid(3), eid(4)]),
        ]);

        run_matching_test(
            index,
            IndexScanInstructions::new([
                IndexScanInstruction::Scan(
                    Arc::new("a".to_owned()),
                    Some(ObjectIdScanPredicate::In([eid(1), eid(3)].into())),
                ),
                scan("b"),
                scan("c"),
                scan("d"),
            ]),
            4,
            2,
        );
    }

    #[tokio::test]
    async fn scan_batches_for_batch_size() {
        let mut index = create_index_with_batch_size(10);
        let mut quads = Vec::new();
        for i in 0..25 {
            quads.push(IndexedQuad([eid(1), eid(2), eid(3), eid(i + 1)]))
        }
        index.insert(quads);

        // The lookup matches a single IndexData that will be scanned.
        run_batch_size_test(
            index,
            IndexScanInstructions::new([
                traverse(1),
                traverse(2),
                traverse(3),
                scan("d"),
            ]),
            &[10, 10, 5],
            true,
        );
    }

    #[tokio::test]
    async fn scan_multi_level_batches_coalesce_results() {
        let mut index = create_index_with_batch_size(10);
        let mut quads = Vec::new();
        for i in 0..25 {
            quads.push(IndexedQuad([eid(1), eid(2), eid(i), eid(3)]))
        }
        index.insert(quads);

        // The lookup matches 25 different IndexLevels, each having exactly one data entry. The
        // batches should be combined into a single batch.
        run_batch_size_test(
            index,
            IndexScanInstructions::new([
                traverse(1),
                traverse(2),
                scan("c"),
                traverse(3),
            ]),
            &[10, 10, 5],
            true,
        );
    }

    #[tokio::test]
    async fn delete_triple_removes_it() {
        let mut index = create_index();
        let quads = vec![
            IndexedQuad([eid(1), eid(2), eid(3), eid(4)]),
            IndexedQuad([eid(1), eid(2), eid(5), eid(6)]),
            IndexedQuad([eid(1), eid(7), eid(3), eid(4)]),
        ];
        index.insert(quads.clone());
        index.remove(quads);

        run_non_matching_test(
            index,
            IndexScanInstructions::new([scan("a"), scan("b"), scan("c"), scan("d")]),
        );
    }

    #[tokio::test]
    async fn delete_triple_non_existing_returns_zero() {
        let mut index = create_index();
        let quads = vec![IndexedQuad([eid(1), eid(2), eid(3), eid(4)])];
        let result = index.remove(quads);
        assert_eq!(result, 0);
    }

    fn create_index() -> MemQuadIndex {
        create_index_with_batch_size(10)
    }

    fn create_index_with_batch_size(batch_size: usize) -> MemQuadIndex {
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
        MemQuadIndex::new(configuration)
    }

    fn traverse(id: u32) -> IndexScanInstruction {
        IndexScanInstruction::Traverse(Some(ObjectIdScanPredicate::In(
            [EncodedObjectId::from(id)].into(),
        )))
    }

    fn scan(name: impl Into<String>) -> IndexScanInstruction {
        IndexScanInstruction::Scan(Arc::new(name.into()), None)
    }

    fn eid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }

    fn run_non_matching_test(index: MemQuadIndex, instructions: IndexScanInstructions) {
        let results = index.scan_quads(instructions).next();
        assert!(
            results.is_none(),
            "Expected no results in non-matching test."
        );
    }

    fn run_matching_test(
        index: MemQuadIndex,
        instructions: IndexScanInstructions,
        expected_columns: usize,
        expected_rows: usize,
    ) {
        let results = index.scan_quads(instructions).next().unwrap();

        assert_eq!(results.num_rows, expected_rows);
        assert_eq!(results.columns.len(), expected_columns);
        for (_, result) in results.columns {
            assert_eq!(result.len(), expected_rows);
        }
    }

    fn run_batch_size_test(
        index: MemQuadIndex,
        instructions: IndexScanInstructions,
        expected_batch_sizes: &[usize],
        ordered: bool,
    ) {
        let mut batch_sizes: Vec<_> = index
            .scan_quads(instructions)
            .map(|arr| arr.num_rows)
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
