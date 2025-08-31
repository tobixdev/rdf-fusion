use crate::memory::encoding::EncodedQuad;
use crate::memory::object_id::{EncodedGraphObjectId, EncodedObjectId};
use crate::memory::storage::index::quad_index::MemQuadIndex;
use crate::memory::storage::index::{
    IndexComponents, IndexConfiguration, IndexScanInstructions, IndexedQuad,
};
use rdf_fusion_common::error::StorageError;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use std::collections::HashSet;
use std::sync::Arc;

/// Represents a set of multiple indexes, each of which indexes a different ordering of the
/// triple component (e.g., SPO, POS). This is necessary as different triple patterns require
/// different index structures.
///
/// For example, the pattern `<S> <P> ?o` can be best served by having an SPO index. The scan would
/// then look up `<S>`, traverse into the next level looking up `<P>`, and lastly scanning the
/// entries and binding them to `?o`. However, the triple pattern `?s <P> <O>` cannot be efficiently
/// evaluated with an SPO index. For this pattern, the query engine should use an POS or OPS index.
///
/// The [IndexSet] allows managing multiple such indices.
#[derive(Debug)]
pub struct IndexSet {
    named_graphs: HashSet<EncodedObjectId>,
    indexes: Vec<MemQuadIndex>,
}

impl IndexSet {
    /// Creates a new [IndexSet] with the given `object_id_encoding` and `batch_size`.
    pub fn new(
        object_id_encoding: ObjectIdEncoding,
        batch_size: usize,
        components: &[IndexComponents],
    ) -> Self {
        let indexes = components
            .iter()
            .map(|components| {
                MemQuadIndex::new(IndexConfiguration {
                    object_id_encoding: object_id_encoding.clone(),
                    batch_size,
                    components: components.clone(),
                })
            })
            .collect();

        Self {
            named_graphs: HashSet::new(),
            indexes,
        }
    }

    /// Returns
    pub fn get_index(
        &self,
        index_configuration: &IndexConfiguration,
    ) -> Option<&MemQuadIndex> {
        self.indexes
            .iter()
            .filter(|index| index.configuration() == index_configuration)
            .next()
    }

    /// Choses the index with the highest scan score (see [Self::compute_scan_score]).
    pub fn choose_index(
        &self,
        pattern: &IndexScanInstructions,
    ) -> (&MemQuadIndex, IndexScanInstructions) {
        let index = self
            .indexes
            .iter()
            .rev() // Prefer SPO (max by uses the last on equality)
            .max_by(|lhs, rhs| {
                let lhs_score =
                    compute_scan_score(&lhs.configuration().components, pattern);
                let rhs_score =
                    compute_scan_score(&rhs.configuration().components, pattern);
                lhs_score.cmp(&rhs_score)
            })
            .expect("At least one index must be available");

        let reordered_pattern =
            reorder_pattern(pattern, &index.configuration().components);
        (index, reordered_pattern)
    }

    pub fn len(&self) -> usize {
        self.any_index().len()
    }

    pub fn insert(&mut self, quads: &[EncodedQuad]) -> Result<usize, StorageError> {
        let mut count = 0;
        for index in self.indexes.iter_mut() {
            let components = index.configuration().components.clone();
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]))
                .map(|q| reorder_quad(&q, &components));
            count = index.insert(quads);
        }

        for quad in quads {
            self.named_graphs.insert(quad.graph_name.0);
        }

        Ok(count)
    }

    pub fn remove(&mut self, quads: &[EncodedQuad]) -> usize {
        let mut count = 0;
        for index in self.indexes.iter_mut() {
            let components = index.configuration().components.clone();
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]))
                .map(|q| reorder_quad(&q, &components));
            count = index.remove(quads);
        }
        count
    }

    pub fn insert_named_graph(&mut self, graph_name: EncodedObjectId) -> bool {
        self.named_graphs.insert(graph_name)
    }

    pub fn named_graphs(&self) -> Vec<EncodedObjectId> {
        self.named_graphs.iter().copied().collect()
    }

    pub fn contains_named_graph(&self, graph_name: EncodedObjectId) -> bool {
        self.named_graphs.contains(&graph_name)
    }

    pub fn clear(&mut self) {
        for index in self.indexes.iter_mut() {
            index.clear();
        }
    }

    pub fn clear_graph(&mut self, graph_name: EncodedGraphObjectId) {
        for index in self.indexes.iter_mut() {
            todo!()
        }
    }

    pub fn drop_named_graph(&mut self, graph_name: EncodedObjectId) -> bool {
        self.clear_graph(EncodedGraphObjectId(graph_name));
        self.named_graphs.remove(&graph_name)
    }

    fn any_index(&self) -> &MemQuadIndex {
        self.indexes.first().unwrap()
    }
}

/// Computes the "scan score" for the given `index_components` and `pattern`.
///
/// The higher the scan score, the better is the index suited for scanning a particular pattern.
/// Basically, this boils down to how many levels can be traversed by looking up bound
/// object ids, prioritizing hits in the "first" levels of the index. The following enumeration
/// shows how the score is computed.
///
/// - 8: The index hits on the first level
/// - 4: The index hits on the second level
/// - 2: The index hits on the third level
/// - 1: The index hits on the fourth level
fn compute_scan_score(
    index_components: &IndexComponents,
    pattern: &IndexScanInstructions,
) -> usize {
    let mut score = 0;

    for (i, index_component) in index_components.inner().iter().enumerate() {
        let idx = index_component.gspo_index();
        let reward = 1 << (index_components.inner().len() - i - 1);
        if pattern.0[idx].predicate().is_some() {
            score += reward
        }
    }

    score as usize
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_quad(pattern: &IndexedQuad, components: &IndexComponents) -> IndexedQuad {
    IndexedQuad([
        pattern.0[components.inner()[0].gspo_index()],
        pattern.0[components.inner()[1].gspo_index()],
        pattern.0[components.inner()[2].gspo_index()],
        pattern.0[components.inner()[3].gspo_index()],
    ])
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_pattern(
    pattern: &IndexScanInstructions,
    components: &IndexComponents,
) -> IndexScanInstructions {
    IndexScanInstructions([
        pattern.0[components.inner()[0].gspo_index()].clone(),
        pattern.0[components.inner()[1].gspo_index()].clone(),
        pattern.0[components.inner()[2].gspo_index()].clone(),
        pattern.0[components.inner()[3].gspo_index()].clone(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::storage::index::{
        IndexScanInstruction, IndexScanInstructions, ObjectIdScanPredicate,
    };
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use std::collections::HashSet;

    #[test]
    fn choose_index_all_bound() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(1),
            traverse_and_filter(2),
            traverse_and_filter(3),
            traverse_and_filter(4),
        ]);

        let (index, new_instructions) = set.choose_index(&pattern);

        assert_eq!(index.configuration().components, IndexComponents::GSPO);
        assert_eq!(
            compute_scan_score(&index.configuration().components, &pattern),
            15
        );
        assert_eq!(new_instructions, pattern)
    }

    #[test]
    fn choose_index_scan_predicate() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(0),
            traverse_and_filter(1),
            IndexScanInstruction::Scan(Arc::new("predicate".to_string()), None),
            traverse_and_filter(3),
        ]);

        let (index, new_instructions) = set.choose_index(&pattern);
        assert_eq!(index.configuration().components, IndexComponents::GOSP);
        assert_eq!(
            compute_scan_score(&index.configuration().components, &pattern),
            14
        );
        assert_eq!(
            new_instructions,
            IndexScanInstructions([
                traverse_and_filter(0),
                traverse_and_filter(3),
                traverse_and_filter(1),
                IndexScanInstruction::Scan(Arc::new("predicate".to_string()), None),
            ])
        )
    }

    #[test]
    fn choose_index_scan_subject_and_object() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(0),
            IndexScanInstruction::Scan(Arc::new("subject".to_string()), None),
            traverse_and_filter(2),
            IndexScanInstruction::Scan(Arc::new("object".to_string()), None),
        ]);

        let (index, new_instructions) = set.choose_index(&pattern);

        assert_eq!(index.configuration().components, IndexComponents::GPOS);
        assert_eq!(
            compute_scan_score(&index.configuration().components, &pattern),
            12
        );
        assert_eq!(
            new_instructions,
            IndexScanInstructions([
                traverse_and_filter(0),
                traverse_and_filter(2),
                IndexScanInstruction::Scan(Arc::new("object".to_string()), None),
                IndexScanInstruction::Scan(Arc::new("subject".to_string()), None),
            ])
        )
    }

    fn create_index_set() -> IndexSet {
        IndexSet::new(
            ObjectIdEncoding::new(4),
            100,
            &[
                IndexComponents::GSPO,
                IndexComponents::GPOS,
                IndexComponents::GOSP,
            ],
        )
    }

    fn traverse_and_filter(id: u32) -> IndexScanInstruction {
        IndexScanInstruction::Traverse(Some(ObjectIdScanPredicate::In(HashSet::from([
            oid(id),
        ]))))
    }

    fn oid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }
}
