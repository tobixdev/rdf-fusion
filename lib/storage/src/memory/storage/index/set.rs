use crate::memory::encoding::EncodedQuad;
use crate::memory::object_id::{EncodedGraphObjectId, EncodedObjectId, DEFAULT_GRAPH_ID};
use crate::memory::storage::index::hash_index::MemHashTripleIndex;
use crate::memory::storage::index::{
    IndexComponents, IndexConfiguration, IndexScanInstructions, IndexedQuad,
};
use crate::memory::storage::VersionNumber;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
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
#[derive(Debug, Clone)]
pub struct IndexSet {
    version_number: VersionNumber,
    indices: Vec<Arc<MemHashTripleIndex>>,
}

impl IndexSet {
    /// Creates a new [IndexSet] with the given `object_id_encoding` and `batch_size`.
    pub fn new(object_id_encoding: ObjectIdEncoding, batch_size: usize) -> Self {
        let indices = vec![
            Arc::new(MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GSPO,
            })),
            Arc::new(MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GPOS,
            })),
            Arc::new(MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GOSP,
            })),
        ];

        Self {
            version_number: VersionNumber(0),
            indices,
        }
    }

    /// Returns the current version number of the index set.
    pub fn version_number(&self) -> VersionNumber {
        self.version_number
    }

    /// Choses the index with the highest scan score (see [Self::compute_scan_score]).
    pub fn choose_index(
        &self,
        pattern: &IndexScanInstructions,
    ) -> (Arc<MemHashTripleIndex>, IndexScanInstructions) {
        let index = self
            .indices
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
        (Arc::clone(index), reordered_pattern)
    }

    pub async fn len(&self) -> Result<usize, StorageError> {
        Ok(self
            .any_index()
            .len(self.version_number)
            .await
            .expect("TODO"))
    }

    pub async fn insert(&mut self, quads: &[EncodedQuad]) -> Result<usize, StorageError> {
        let version_number = self.version_number.next();

        let mut count = 0;
        for index in self.indices.iter_mut() {
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]))
                .map(|q| reorder_quad(&q, &index.configuration().components));
            count = index.insert(quads, version_number).await.expect("TODO");
        }

        self.version_number = version_number;
        Ok(count)
    }

    pub async fn remove(&mut self, quads: &[EncodedQuad]) -> Result<usize, StorageError> {
        let version_number = self.version_number.next();

        let mut count = 0;
        for index in self.indices.iter_mut() {
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]))
                .map(|q| reorder_quad(&q, &index.configuration().components));
            count = index.remove(quads, version_number).await.expect("TODO")
        }

        self.version_number = version_number;
        Ok(count)
    }

    pub async fn insert_named_graph<'a>(
        &mut self,
        graph_name: EncodedObjectId,
    ) -> Result<bool, StorageError> {
        let mut result = false;

        let version_number = self.version_number.next();
        for index in self.indices.iter_mut() {
            if !index.configuration().components.is_graph_name_top_level() {
                continue;
            }

            result = index
                .insert_top_level(graph_name, version_number)
                .await
                .expect("TODO");
        }

        self.version_number = version_number;
        Ok(result)
    }

    pub async fn named_graphs(&self) -> Result<Vec<EncodedObjectId>, StorageError> {
        let mut inner_result = self
            .any_index_with_graph_name_top_level()
            .scan_top_level(self.version_number)
            .await
            .expect("TODO");
        inner_result.retain(|oid| *oid != DEFAULT_GRAPH_ID.0);
        Ok(inner_result)
    }

    pub async fn contains_named_graph<'a>(
        &self,
        graph_name: EncodedObjectId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .any_index_with_graph_name_top_level()
            .contains_top_level(graph_name, self.version_number)
            .await
            .expect("TODO"))
    }

    pub async fn clear(&mut self) -> Result<(), StorageError> {
        let version_number = self.version_number.next();

        for index in self.indices.iter_mut() {
            index.clear(version_number).await.expect("TODO");
        }

        self.version_number = version_number;
        Ok(())
    }

    pub async fn clear_graph<'a>(
        &mut self,
        graph_name: EncodedGraphObjectId,
    ) -> Result<(), StorageError> {
        let version_number = self.version_number.next();

        // TODO: this does not handle non-top level graph names

        for index in self.indices.iter_mut() {
            index
                .clear_top_level(graph_name.0, version_number)
                .await
                .expect("TODO");
        }

        self.version_number = version_number;
        Ok(())
    }

    pub async fn drop_named_graph(
        &mut self,
        graph_name: EncodedObjectId,
    ) -> Result<bool, StorageError> {
        let version_number = self.version_number.next();

        // TODO: this does not handle non-top level graph names

        let mut result = false;
        for index in self.indices.iter_mut() {
            result = index
                .drop_top_level(graph_name, version_number)
                .await
                .expect("TODO");
        }

        self.version_number = version_number;
        Ok(result)
    }

    fn any_index_with_graph_name_top_level(&self) -> &MemHashTripleIndex {
        self.indices
            .iter()
            .filter(|index| index.configuration().components.is_graph_name_top_level())
            .next()
            .unwrap()
    }

    fn any_index(&self) -> &MemHashTripleIndex {
        self.indices.iter().next().unwrap()
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
        pattern.0[components.inner()[0].gspo_index()].clone(),
        pattern.0[components.inner()[1].gspo_index()].clone(),
        pattern.0[components.inner()[2].gspo_index()].clone(),
        pattern.0[components.inner()[3].gspo_index()].clone(),
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

    #[tokio::test]
    async fn choose_index_all_bound() {
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

    #[tokio::test]
    async fn choose_index_scan_predicate() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(0),
            traverse_and_filter(1),
            IndexScanInstruction::Scan("predicate".to_string(), None),
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
                IndexScanInstruction::Scan("predicate".to_string(), None),
            ])
        )
    }

    #[tokio::test]
    async fn choose_index_scan_subject_and_object() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(0),
            IndexScanInstruction::Scan("subject".to_string(), None),
            traverse_and_filter(2),
            IndexScanInstruction::Scan("object".to_string(), None),
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
                IndexScanInstruction::Scan("subject".to_string(), None),
                IndexScanInstruction::Scan("object".to_string(), None),
            ])
        )
    }

    fn create_index_set() -> IndexSet {
        IndexSet::new(ObjectIdEncoding::new(4), 100)
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
