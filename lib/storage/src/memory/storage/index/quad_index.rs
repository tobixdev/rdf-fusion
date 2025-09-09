use crate::memory::object_id::EncodedGraphObjectId;
use crate::memory::storage::index::quad_index_data::IndexData;
use crate::memory::storage::index::scan::MemQuadIndexScanIterator;
use crate::memory::storage::index::{
    DirectIndexRef, IndexConfiguration, IndexScanInstructions, IndexedQuad,
    PruningPredicate, PruningPredicates,
};
use std::collections::BTreeSet;

/// TODO
#[derive(Debug)]
pub struct MemQuadIndex {
    /// The index content.
    data: IndexData,
    /// The configuration of the index.
    configuration: IndexConfiguration,
}

impl MemQuadIndex {
    /// Creates a new [MemQuadIndex].
    pub fn new(configuration: IndexConfiguration) -> Self {
        let nullable_position = configuration
            .components
            .inner()
            .iter()
            .position(|c| c.gspo_index() == 0)
            .expect("There has to be a graph name");
        Self {
            data: IndexData::new(configuration.batch_size, nullable_position),
            configuration,
        }
    }

    /// Returns a reference to the content of the index.
    pub(super) fn data(&self) -> &IndexData {
        &self.data
    }

    /// Returns a reference to the index configuration.
    pub fn configuration(&self) -> &IndexConfiguration {
        &self.configuration
    }

    /// Returns the total number of quads.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Inserts a list of quads.
    ///
    /// Quads that already exist in the index are ignored.
    pub fn insert(&mut self, quads: impl IntoIterator<Item = IndexedQuad>) -> usize {
        let mut to_insert = BTreeSet::new();

        for quad in quads {
            to_insert.insert(quad);
        }

        self.data.insert(&to_insert)
    }

    /// Removes a list of quads.
    ///
    /// Quads that do not exist in the index are ignored.
    pub fn remove(&mut self, quads: impl IntoIterator<Item = IndexedQuad>) -> usize {
        let mut to_insert = BTreeSet::new();

        for quad in quads {
            to_insert.insert(quad);
        }

        self.data.remove(&to_insert)
    }

    /// TODO
    pub fn clear(&mut self) {
        self.data =
            IndexData::new(self.configuration.batch_size, self.data.nullable_position());
    }

    /// TODO
    pub fn clear_graph(&mut self, graph_name: EncodedGraphObjectId) {
        let index = self.data.nullable_position();
        self.data
            .clear_all_with_value_in_column(graph_name.0, index);
    }

    /// TODO
    pub fn scan_quads(
        &self,
        instructions: IndexScanInstructions,
    ) -> MemQuadIndexScanIterator<DirectIndexRef<'_>> {
        MemQuadIndexScanIterator::new(self, instructions)
    }

    /// Computes the "scan score" for the given `instructions`.
    ///
    /// The higher the scan score, the better is the index suited for scanning a particular pattern.
    /// Basically, this boils down to how many levels can be traversed by looking up bound
    /// object ids.
    pub fn compute_scan_score(&self, instructions: &IndexScanInstructions) -> usize {
        let pruning_predicates = PruningPredicates::from(instructions);
        let mut score = 0;

        for (i, predicate) in pruning_predicates.0.iter().enumerate() {
            let Some(predicate) = predicate else {
                break;
            };

            let potent = (instructions.0.len() - i) * 2;
            let reward = match predicate {
                PruningPredicate::EqualTo(_) => 2,
                PruningPredicate::Between(_, _) => 1,
            };

            score += reward << potent;
        }

        score
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::index::components::IndexComponent;
    use crate::memory::storage::index::{
        IndexComponents, IndexConfiguration, IndexScanInstruction, IndexScanInstructions,
        ObjectIdScanPredicate,
    };
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use std::sync::Arc;

    #[test]
    fn test_in_predicate_better_than_nothing() {
        let idx = make_index();

        let eq = IndexScanInstructions::new([
            IndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(ObjectIdScanPredicate::In(
                    [EncodedObjectId::from(10)].into(),
                )),
            ),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let nothing = IndexScanInstructions::new([
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let eq_score = idx.compute_scan_score(&eq);
        let nothing_score = idx.compute_scan_score(&nothing);

        assert!(
            eq_score > nothing_score,
            "EqualTo should provide a better scan score than nothing"
        );
    }

    #[test]
    fn test_in_predicate_following_none_equal_to_nothing() {
        let idx = make_index();

        let eq = IndexScanInstructions::new([
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(ObjectIdScanPredicate::In(
                    [EncodedObjectId::from(10)].into(),
                )),
            ),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let nothing = IndexScanInstructions::new([
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
        ]);

        let eq_score = idx.compute_scan_score(&eq);
        let nothing_score = idx.compute_scan_score(&nothing);

        assert_eq!(
            eq_score, nothing_score,
            "In should provide a better scan score than nothing"
        );
    }

    #[test]
    fn test_in_predicate_better_than_between() {
        let idx = make_index();
        let instructions_eq = IndexScanInstructions::new([
            IndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(ObjectIdScanPredicate::In(
                    [EncodedObjectId::from(10)].into(),
                )),
            ),
            IndexScanInstruction::Scan(
                Arc::new("s".to_string()),
                Some(ObjectIdScanPredicate::In(
                    [EncodedObjectId::from(10)].into(),
                )),
            ),
            IndexScanInstruction::Scan(Arc::new("p".to_string()), None),
            IndexScanInstruction::Scan(Arc::new("o".to_string()), None),
        ]);

        let instructions_mixed = IndexScanInstructions::new([
            IndexScanInstruction::Scan(
                Arc::new("g".to_string()),
                Some(ObjectIdScanPredicate::EqualTo(Arc::new("x".to_string()))),
            ),
            IndexScanInstruction::Scan(
                Arc::new("s".to_string()),
                Some(ObjectIdScanPredicate::Between(
                    EncodedObjectId::from(1),
                    EncodedObjectId::from(10),
                )),
            ),
            IndexScanInstruction::Scan(Arc::new("p".to_string()), None),
            IndexScanInstruction::Scan(Arc::new("o".to_string()), None),
        ]);

        let eq_score = idx.compute_scan_score(&instructions_eq);
        let mixed_score = idx.compute_scan_score(&instructions_mixed);

        assert!(
            eq_score > mixed_score,
            "Full EqualTo score should be higher than mixed"
        );
    }

    fn make_index() -> MemQuadIndex {
        let components = IndexComponents::try_new([
            IndexComponent::GraphName,
            IndexComponent::Subject,
            IndexComponent::Predicate,
            IndexComponent::Object,
        ])
        .unwrap();
        let config = IndexConfiguration {
            object_id_encoding: ObjectIdEncoding::new(4),
            batch_size: 128,
            components,
        };
        MemQuadIndex::new(config)
    }
}
