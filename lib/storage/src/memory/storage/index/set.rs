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
    gspo: Arc<MemHashTripleIndex>,
}

impl IndexSet {
    /// Creates a new [IndexSet] with the given `object_id_encoding` and `batch_size`.
    pub fn new(object_id_encoding: ObjectIdEncoding, batch_size: usize) -> Self {
        Self {
            version_number: VersionNumber(0),
            gspo: Arc::new(MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GSPO,
            })),
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
    ) -> Arc<MemHashTripleIndex> {
        let index = [&self.gspo]
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
        Ok(self.gspo.len(self.version_number).await.expect("TODO"))
    }

    pub async fn insert(&mut self, quads: &[EncodedQuad]) -> Result<usize, StorageError> {
        let version_number = self.version_number.next();

        let mut count = 0;
        for index in [&self.gspo] {
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]));
            count = index.insert(quads, version_number).await.expect("TODO");
        }

        self.version_number = version_number;
        Ok(count)
    }

    pub async fn remove(&mut self, quads: &[EncodedQuad]) -> Result<usize, StorageError> {
        let version_number = self.version_number.next();

        let mut count = 0;
        for index in [&self.gspo] {
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]));
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
        for index in [&self.gspo] {
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
            .gspo
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
            .gspo
            .contains_top_level(graph_name, self.version_number)
            .await
            .expect("TODO"))
    }

    pub async fn clear(&mut self) -> Result<(), StorageError> {
        let version_number = self.version_number.next();

        self.gspo.clear(version_number).await.expect("TODO");

        self.version_number = version_number;
        Ok(())
    }

    pub async fn clear_graph<'a>(
        &mut self,
        graph_name: EncodedGraphObjectId,
    ) -> Result<(), StorageError> {
        let version_number = self.version_number.next();

        self.gspo
            .clear_top_level(graph_name.0, version_number)
            .await
            .expect("TODO");

        self.version_number = version_number;
        Ok(())
    }

    pub async fn drop_named_graph(
        &mut self,
        graph_name: EncodedObjectId,
    ) -> Result<bool, StorageError> {
        let version_number = self.version_number.next();

        let result = self
            .gspo
            .drop_top_level(graph_name, version_number)
            .await
            .expect("TODO");

        self.version_number = version_number;
        Ok(result)
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
