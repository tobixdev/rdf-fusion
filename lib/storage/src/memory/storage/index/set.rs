use crate::memory::storage::index::error::IndexScanError;
use crate::memory::storage::index::hash_index::{
    MemHashIndexIterator, MemHashTripleIndex,
};
use crate::memory::storage::index::{
    IndexComponents, IndexConfiguration, IndexScanInstructions,
};
use crate::memory::storage::VersionNumber;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_model::{
    GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
};
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
    spo: Arc<MemHashTripleIndex>,
    pos: Arc<MemHashTripleIndex>,
    ops: Arc<MemHashTripleIndex>,
}

impl IndexSet {
    /// Creates a new [IndexSet] with the given `object_id_encoding` and `batch_size`.
    pub fn new(object_id_encoding: ObjectIdEncoding, batch_size: usize) -> Self {
        Self {
            version_number: VersionNumber(0),
            spo: Arc::new(MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GSPO,
            })),
            pos: Arc::new(MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GPOS,
            })),
            ops: Arc::new(MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GOSP,
            })),
        }
    }

    /// Returns the current version number of the index set.
    pub fn version_number(&self) -> VersionNumber {
        self.version_number
    }

    /// Choses the index with the highest scan score (see [Self::compute_scan_score]).
    pub fn choose_index(&self, pattern: &IndexScanInstructions) -> Arc<MemHashTripleIndex> {
        let index = [&self.spo, &self.pos, &self.ops]
            .into_iter()
            .max_by(|lhs, rhs| {
                let lhs_score =
                    Self::compute_scan_score(&lhs.configuration().components, pattern);
                let rhs_score =
                    Self::compute_scan_score(&rhs.configuration().components, pattern);
                lhs_score.cmp(&rhs_score)
            })
            .expect("At least one index must be available");
        Arc::clone(index)
    }

    /// Computes the "scan score" for the given `index_components` and `pattern`.
    ///
    /// The higher the scan score, the better is the index suited for scanning a particular pattern.
    /// Basically, this boils down to how many levels can be traversed by looking up bound
    /// object ids, prioritizing hits in the "first" levels of the index. The following enumeration
    /// shows how the score is computed.
    ///
    /// - 1000: The index hits on the first level
    /// - 100: The index hits on the second level
    /// - 10: The index hits on the third level
    /// - 1: The index hits on the fourth level
    fn compute_scan_score(
        index_components: &IndexComponents,
        pattern: &IndexScanInstructions,
    ) -> usize {
        let mut score = 0;

        for (i, index_component) in index_components.inner().iter().enumerate() {
            let idx = index_component.gspo_index();
            let reward = 10u32.pow((index_components.inner().len() - i) as u32);
            let is_filtered = pattern.0[idx]
                .predicate()
                .map(|p| p.restricts_to_known_size())
                .unwrap_or(false);

            if is_filtered {
                score += reward
            } else {
                break;
            }
        }

        score as usize
    }

    pub async fn len(&self) -> Result<usize, StorageError> {
        todo!()
    }

    pub async fn insert(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        todo!()
    }

    pub async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        todo!()
    }

    pub async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        todo!()
    }

    pub async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        todo!()
    }

    pub async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        todo!()
    }

    pub async fn clear(&self) -> Result<(), StorageError> {
        todo!()
    }

    pub async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        todo!()
    }

    pub async fn drop_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        todo!()
    }
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_pattern(
    pattern: &IndexScanInstructions,
    components: &IndexComponents,
) -> IndexScanInstructions {
    let mut new_lookup = Vec::new();

    for i in 0..4 {
        let gspo_index = components
            .inner()
            .iter()
            .position(|c| c.gspo_index() == i)
            .expect("There must be a component with each index.");
        new_lookup.push(pattern.0[gspo_index].clone());
    }

    IndexScanInstructions(new_lookup.try_into().unwrap())
}
