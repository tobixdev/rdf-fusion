use crate::memory::encoding::EncodedObjectIdPattern;
use crate::memory::storage::VersionNumber;
use crate::memory::storage::index::error::IndexScanError;
use crate::memory::storage::index::index_data::{
    IndexLookup, MemHashTripleIndex, MemHashTripleIndexIterator,
};
use crate::memory::storage::index::{IndexComponents, IndexConfiguration};
use rdf_fusion_encoding::object_id::ObjectIdEncoding;

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
pub struct IndexSet {
    spo: MemHashTripleIndex,
    pos: MemHashTripleIndex,
    ops: MemHashTripleIndex,
}

impl IndexSet {
    /// Creates a new [IndexSet] with the given `object_id_encoding` and `batch_size`.
    pub fn new(object_id_encoding: ObjectIdEncoding, batch_size: usize) -> Self {
        Self {
            spo: MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GSPO,
            }),
            pos: MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GPOS,
            }),
            ops: MemHashTripleIndex::new(IndexConfiguration {
                object_id_encoding: object_id_encoding.clone(),
                batch_size,
                components: IndexComponents::GOSP,
            }),
        }
    }

    /// Scans the best matching index for the given triple pattern. The patterns should *always* be
    /// given as subject, predicate, object. The function will re-order the patterns for the chosen
    /// index to scan.
    pub async fn scan(
        &self,
        pattern: IndexLookup,
        version_number: VersionNumber,
    ) -> Result<MemHashTripleIndexIterator, IndexScanError> {
        let chosen_index = self.choose_index(&pattern);
        chosen_index.scan(pattern, version_number).await
    }

    /// Choses the index with the highest scan score (see [Self::compute_scan_score]).
    fn choose_index(&self, pattern: &IndexLookup) -> &MemHashTripleIndex {
        [&self.spo, &self.pos, &self.ops]
            .iter()
            .max_by(|lhs, rhs| {
                let lhs_score =
                    Self::compute_scan_score(&lhs.configuration().components, pattern);
                let rhs_score =
                    Self::compute_scan_score(&rhs.configuration().components, pattern);
                lhs_score.cmp(&rhs_score)
            })
            .expect("At least one index must be available")
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
        pattern: &IndexLookup,
    ) -> usize {
        let mut score = 0;

        for (i, index_component) in index_components.0.iter().enumerate() {
            let idx = index_component.gspo_index();
            let is_bound = pattern.0[idx].is_bound();
            let reward = 10u32.pow((index_components.0.len() - i) as u32);
            if is_bound {
                score += reward
            } else {
                break;
            }
        }

        score as usize
    }
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_pattern(pattern: &IndexLookup, components: &IndexComponents) -> IndexLookup {
    let mut new_lookup = [EncodedObjectIdPattern::Variable; 4];

    for (i, lookup) in new_lookup.iter_mut().enumerate() {
        let gspo_index = components.0[i].gspo_index();
        *lookup = pattern.0[gspo_index];
    }

    IndexLookup(new_lookup)
}
