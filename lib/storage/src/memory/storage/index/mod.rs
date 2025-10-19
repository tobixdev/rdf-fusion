use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use std::collections::{BTreeSet, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

mod components;
mod error;
mod quad_index;
mod quad_index_data;
mod scan;
mod set;

use crate::memory::encoding::{EncodedActiveGraph, EncodedTermPattern};
use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedObjectId};
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

/// A list of [IndexScanInstruction]s for querying a quad index.
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
                            IndexScanPredicate::EqualTo(Arc::clone(&var)).into(),
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

    /// Returns the inner [IndexScanInstruction]s.
    pub fn instructions(&self) -> &[IndexScanInstruction; 4] {
        &self.0
    }

    /// Tries to find the [IndexScanInstruction] for a given column name.
    ///
    /// It should not be possible for two instructions to have the same name, as the second
    /// instruction should have been turned into a predicate.
    pub fn instructions_for_column(
        &self,
        column: &str,
    ) -> Option<(usize, &IndexScanInstruction)> {
        self.0
            .iter()
            .enumerate()
            .find(|(_, i)| i.scan_variable() == Some(column))
    }

    /// Returns new [IndexScanInstructions] with the given `instruction` at the given `index`.
    ///
    /// # Panics
    ///
    /// Will panic if the index is out-of-range.
    pub fn with_new_instruction_at(
        self,
        index: usize,
        instruction: IndexScanInstruction,
    ) -> Self {
        let mut new_instructions = self.0;
        new_instructions[index] = instruction;
        Self(new_instructions)
    }

    pub fn snapshot(&self) -> IndexScanInstructionsSnapshot {
        IndexScanInstructionsSnapshot(self.0.clone().map(|i| i.snapshot()))
    }
}

/// A snapshot of an [IndexScanInstructions].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexScanInstructionsSnapshot([IndexScanInstructionSnapshot; 4]);

impl IndexScanInstructionsSnapshot {
    /// Create a new [IndexScanInstructionsSnapshot].
    pub fn new(instructions: [IndexScanInstructionSnapshot; 4]) -> Self {
        Self(instructions)
    }
}

/// A predicate for filtering object ids.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum IndexScanPredicate {
    /// Always returns false.
    False,
    /// Checks whether the object id is in the given set.
    In(BTreeSet<EncodedObjectId>),
    /// Checks whether the object id is between the given object ids (end is inclusive).
    Between(EncodedObjectId, EncodedObjectId),
    /// Checks whether the object id is equal to the scan instruction with the given variable.
    EqualTo(Arc<String>),
}

impl IndexScanPredicate {
    /// Combines this predicate with `other` using a logical and.
    pub fn try_and_with(&self, other: &IndexScanPredicate) -> Option<IndexScanPredicate> {
        use IndexScanPredicate::*;
        let result = match (self, other) {
            // False with any predicate is false.
            (_, False) | (False, _) => False,

            // Intersect the sets.
            (In(a), In(b)) => {
                let inter: BTreeSet<_> = a.intersection(b).cloned().collect();
                if inter.is_empty() { False } else { In(inter) }
            }

            // For In, we can simply filter based on the other predicate.
            (In(a), Between(f, t)) | (Between(f, t), In(a)) => {
                let filtered: BTreeSet<_> = a
                    .iter()
                    .filter(|v| **v >= *f && **v <= *t)
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    False
                } else {
                    In(filtered)
                }
            }

            // Intersect between ranges
            (Between(from_1, to_1), Between(from_2, to_2)) => {
                let from = (*from_1).max(*from_2);
                let to = (*to_1).min(*to_2);
                if from > to { False } else { Between(from, to) }
            }

            // Otherwise, return None to indicate that the predicates cannot be combined.
            _ => return None,
        };
        Some(result)
    }
}

impl Display for IndexScanPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexScanPredicate::False => f.write_str("false"),
            IndexScanPredicate::In(set) => {
                if set.len() == 1 {
                    write!(f, "== {}", set.iter().next().unwrap())
                } else {
                    write!(
                        f,
                        "in ({})",
                        set.iter()
                            .map(ToString::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            }
            IndexScanPredicate::Between(from, to) => {
                write!(f, "in ({from}..{to})")
            }
            IndexScanPredicate::EqualTo(column) => write!(f, "== {column}"),
        }
    }
}

/// TODO
pub trait IndexScanPredicateSource: Debug + Send + Sync + Display {
    /// TODO
    fn current_predicate(&self) -> IndexScanPredicate;
}

/// TODO
#[derive(Debug, Clone)]
pub enum PossiblyDynamicIndexScanPredicate {
    /// TODO
    Static(IndexScanPredicate),
    /// TODO
    Dynamic(Arc<dyn IndexScanPredicateSource>),
}

impl PossiblyDynamicIndexScanPredicate {
    /// Combines this predicate with `other` using a logical and.
    ///
    /// Currently, only static predicates will be combined.
    pub fn try_and_with(
        &self,
        other: &PossiblyDynamicIndexScanPredicate,
    ) -> Option<Self> {
        use PossiblyDynamicIndexScanPredicate::*;

        match (self, other) {
            (Static(left), Static(right)) => Some(Static(left.try_and_with(right)?)),
            _ => None,
        }
    }

    /// TODO
    pub fn snapshot(&self) -> IndexScanPredicate {
        match self {
            PossiblyDynamicIndexScanPredicate::Static(predicate) => predicate.clone(),
            PossiblyDynamicIndexScanPredicate::Dynamic(predicate) => {
                predicate.current_predicate()
            }
        }
    }
}

impl From<IndexScanPredicate> for PossiblyDynamicIndexScanPredicate {
    fn from(value: IndexScanPredicate) -> Self {
        Self::Static(value)
    }
}

impl PartialEq for PossiblyDynamicIndexScanPredicate {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Static(left), Self::Static(right)) => left == right,
            (Self::Dynamic(left), Self::Dynamic(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

impl Eq for PossiblyDynamicIndexScanPredicate {}

impl Display for PossiblyDynamicIndexScanPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PossiblyDynamicIndexScanPredicate::Static(predicate) => {
                write!(f, "{predicate}")
            }
            PossiblyDynamicIndexScanPredicate::Dynamic(predicate) => {
                write!(f, "dynamic<{predicate}>")
            }
        }
    }
}

/// An encoded version of a triple pattern.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IndexScanInstruction {
    /// Traverses the index level, not binding the elements at this level.
    Traverse(Option<PossiblyDynamicIndexScanPredicate>),
    /// Scans the index level, binding the elements at this level.
    Scan(Arc<String>, Option<PossiblyDynamicIndexScanPredicate>),
}

impl IndexScanInstruction {
    /// Creates a new [IndexScanInstruction::Traverse].
    pub fn traverse() -> Self {
        IndexScanInstruction::Traverse(None)
    }

    /// Creates a new [IndexScanInstruction::Traverse] with the given predicate.
    pub fn traverse_with_predicate(
        predicate: impl Into<PossiblyDynamicIndexScanPredicate>,
    ) -> Self {
        IndexScanInstruction::Traverse(Some(predicate.into()))
    }

    /// Creates a new [IndexScanInstruction::Scan].
    pub fn scan(variable: impl Into<Arc<String>>) -> Self {
        IndexScanInstruction::Scan(variable.into(), None)
    }

    /// Creates a new [IndexScanInstruction::Scan] with the given predicate.
    pub fn scan_with_predicate(
        variable: impl Into<Arc<String>>,
        predicate: impl Into<PossiblyDynamicIndexScanPredicate>,
    ) -> Self {
        IndexScanInstruction::Scan(variable.into(), Some(predicate.into()))
    }

    /// Returns the scan variable (i.e., the variable to bind the results to) for this instruction.
    pub fn scan_variable(&self) -> Option<&str> {
        match self {
            IndexScanInstruction::Traverse(_) => None,
            IndexScanInstruction::Scan(variable, _) => Some(variable.as_str()),
        }
    }

    /// Returns the predicate for this instruction.
    pub fn predicate(&self) -> Option<&PossiblyDynamicIndexScanPredicate> {
        match self {
            IndexScanInstruction::Traverse(predicate) => predicate.as_ref(),
            IndexScanInstruction::Scan(_, predicate) => predicate.as_ref(),
        }
    }

    /// Creates a new [IndexScanInstruction] that has no predicate, even if the original instruction
    /// contained a predicate.
    pub fn without_predicate(self) -> Self {
        match self {
            IndexScanInstruction::Traverse(_) => IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Scan(variable, _) => {
                IndexScanInstruction::Scan(variable, None)
            }
        }
    }

    /// Creates a new [IndexScanInstruction] with the given new predicate.
    pub fn with_predicate(self, predicate: PossiblyDynamicIndexScanPredicate) -> Self {
        match self {
            IndexScanInstruction::Traverse(_) => {
                IndexScanInstruction::Traverse(Some(predicate))
            }
            IndexScanInstruction::Scan(variable, _) => {
                IndexScanInstruction::Scan(variable, Some(predicate))
            }
        }
    }

    /// Creates an [IndexScanInstructionSnapshot] from this instruction.
    pub fn snapshot(&self) -> IndexScanInstructionSnapshot {
        match self {
            IndexScanInstruction::Traverse(predicate) => {
                let predicate = predicate.as_ref().map(|p| p.snapshot());
                IndexScanInstructionSnapshot::Traverse(predicate)
            }
            IndexScanInstruction::Scan(name, predicate) => {
                let predicate = predicate.as_ref().map(|p| p.snapshot());
                IndexScanInstructionSnapshot::Scan(Arc::clone(name), predicate)
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
        let instruction_with_predicate =
            |predicate: Option<PossiblyDynamicIndexScanPredicate>| {
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
                instruction_with_predicate(Some(
                    IndexScanPredicate::In(object_ids).into(),
                ))
            }
            EncodedActiveGraph::AllGraphs => instruction_with_predicate(None),
            EncodedActiveGraph::Union(graphs) => {
                let object_ids = BTreeSet::from_iter(graphs.iter().map(|g| g.0));
                instruction_with_predicate(Some(
                    IndexScanPredicate::In(object_ids).into(),
                ))
            }
            EncodedActiveGraph::AnyNamedGraph => instruction_with_predicate(Some(
                IndexScanPredicate::Between(
                    DEFAULT_GRAPH_ID.0.next().unwrap(),
                    EncodedObjectId::MAX,
                )
                .into(),
            )),
        }
    }
}

/// A snapshot of an [IndexScanInstruction].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexScanInstructionSnapshot {
    /// Traverses the index level, not binding the elements at this level.
    Traverse(Option<IndexScanPredicate>),
    /// Scans the index level, binding the elements at this level.
    Scan(Arc<String>, Option<IndexScanPredicate>),
}

impl IndexScanInstructionSnapshot {
    /// Returns a reference to the predicate for this instruction.
    pub fn predicate(&self) -> Option<&IndexScanPredicate> {
        match self {
            IndexScanInstructionSnapshot::Traverse(predicate) => predicate.as_ref(),
            IndexScanInstructionSnapshot::Scan(_, predicate) => predicate.as_ref(),
        }
    }

    /// Creates a new [IndexScanInstructionSnapshot] that has no predicate, even if the original
    /// instruction contained a predicate.
    pub fn without_predicate(self) -> Self {
        match self {
            IndexScanInstructionSnapshot::Traverse(_) => {
                IndexScanInstructionSnapshot::Traverse(None)
            }
            IndexScanInstructionSnapshot::Scan(variable, _) => {
                IndexScanInstructionSnapshot::Scan(variable, None)
            }
        }
    }
}

impl From<EncodedTermPattern> for IndexScanInstruction {
    fn from(value: EncodedTermPattern) -> Self {
        match value {
            EncodedTermPattern::ObjectId(object_id) => IndexScanInstruction::Traverse(
                Some(IndexScanPredicate::In(BTreeSet::from([object_id])).into()),
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

impl From<&IndexScanInstructionsSnapshot> for PruningPredicates {
    fn from(value: &IndexScanInstructionsSnapshot) -> Self {
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

impl From<&IndexScanPredicate> for Option<PruningPredicate> {
    fn from(value: &IndexScanPredicate) -> Self {
        match value {
            IndexScanPredicate::In(ids) => {
                let predicate = if ids.len() == 1 {
                    PruningPredicate::EqualTo(*ids.first().unwrap())
                } else {
                    PruningPredicate::Between(*ids.first().unwrap(), *ids.last().unwrap())
                };
                Some(predicate)
            }
            IndexScanPredicate::Between(from, to) => {
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
                    Some(IndexScanPredicate::In([eid(1), eid(3)].into()).into()),
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

    #[test]
    fn try_and_with_false_predicate_returns_false() {
        let in_pred = IndexScanPredicate::In([eid(1), eid(2)].into());
        let between_pred = IndexScanPredicate::Between(eid(1), eid(5));
        let false_pred = IndexScanPredicate::False;

        // False with anything is False
        assert_eq!(
            in_pred.try_and_with(&false_pred),
            Some(IndexScanPredicate::False)
        );
        assert_eq!(
            false_pred.try_and_with(&between_pred),
            Some(IndexScanPredicate::False)
        );
    }

    #[test]
    fn try_and_with_in_and_in_intersects() {
        let a = IndexScanPredicate::In([eid(1), eid(2), eid(3)].into());
        let b = IndexScanPredicate::In([eid(2), eid(3), eid(4)].into());
        assert_eq!(
            a.try_and_with(&b),
            Some(IndexScanPredicate::In([eid(2), eid(3)].into()))
        );
    }

    #[test]
    fn try_and_with_in_and_in_disjoint_returns_false() {
        let a = IndexScanPredicate::In([eid(1)].into());
        let b = IndexScanPredicate::In([eid(2)].into());
        assert_eq!(a.try_and_with(&b), Some(IndexScanPredicate::False));
    }

    #[test]
    fn try_and_with_in_and_between_filters_in_set() {
        let a = IndexScanPredicate::In([eid(1), eid(2), eid(3)].into());
        let b = IndexScanPredicate::Between(eid(2), eid(3));
        // Only 2 and 3 fall into the range
        assert_eq!(
            a.try_and_with(&b),
            Some(IndexScanPredicate::In([eid(2), eid(3)].into()))
        );
    }

    #[test]
    fn try_and_with_between_and_in_filters_in_set() {
        let a = IndexScanPredicate::Between(eid(2), eid(4));
        let b = IndexScanPredicate::In([eid(3), eid(4), eid(5)].into());
        // Only 3 and 4 fall into both
        assert_eq!(
            a.try_and_with(&b),
            Some(IndexScanPredicate::In([eid(3), eid(4)].into()))
        );
    }

    #[test]
    fn try_and_with_between_and_between_intersects() {
        let a = IndexScanPredicate::Between(eid(2), eid(5));
        let b = IndexScanPredicate::Between(eid(3), eid(4));
        // Intersection is 3 to 4
        assert_eq!(
            a.try_and_with(&b),
            Some(IndexScanPredicate::Between(eid(3), eid(4)))
        );
    }

    #[test]
    fn try_and_with_between_and_between_disjoint_returns_false() {
        let a = IndexScanPredicate::Between(eid(1), eid(2));
        let b = IndexScanPredicate::Between(eid(3), eid(4));
        // Disjoint
        assert_eq!(a.try_and_with(&b), Some(IndexScanPredicate::False));
    }

    #[test]
    fn try_and_with_incompatible_returns_none() {
        let a = IndexScanPredicate::In([eid(1)].into());
        let b = IndexScanPredicate::EqualTo(Arc::new("x".to_string()));
        assert_eq!(a.try_and_with(&b), None);
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
        IndexScanInstruction::Traverse(Some(
            IndexScanPredicate::In([EncodedObjectId::from(id)].into()).into(),
        ))
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
