use crate::memory::storage::log::MemLogSnapshot;
use crate::memory::MemObjectIdMapping;
use rdf_fusion_model::NamedOrBlankNodeRef;
use std::sync::Arc;

/// Provides a snapshot view on the storage. Other transactions can read and write to the storage
/// without changing the view of the snapshot.
pub struct MemQuadStorageSnapshot {
    /// Object id mapping
    object_id_mapping: Arc<MemObjectIdMapping>,
    /// A snapshot of the log. This has a sequence of all changes to the system.
    log_snapshot: MemLogSnapshot,
}

impl MemQuadStorageSnapshot {
    /// Create a new [MemQuadStorageSnapshot].
    pub fn new(
        object_id_mapping: Arc<MemObjectIdMapping>,
        log_snapshot: MemLogSnapshot,
    ) -> Self {
        Self {
            object_id_mapping,
            log_snapshot,
        }
    }

    /// Returns the number of quads in the storage.
    pub async fn len(&self) -> usize {
        let changes = self.log_snapshot.count_changes().await;
        changes.insertions - changes.deletions
    }

    /// Returns whether the storage contains the named graph `graph_name`.
    pub async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> bool {
        let Some(object_id) = self
            .object_id_mapping
            .try_get_encoded_object_id_from_term(graph_name)
        else {
            // Object IDs are not garbage collected. If the object ID is not found, the name has
            // never been stored in the system.
            return false;
        };

        self.log_snapshot.contains_named_graph(object_id).await
    }
}
