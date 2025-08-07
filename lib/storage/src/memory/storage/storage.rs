use crate::memory::storage::log::MemLog;
use crate::memory::storage::snapshot::MemQuadStorageSnapshot;
use crate::memory::MemObjectIdMapping;
use async_trait::async_trait;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_api::storage::QuadStorage;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::{
    GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
};
use std::sync::Arc;

/// A memory-based quad storage.
pub struct MemQuadStorage {
    /// Holds the mapping between terms and object ids.
    object_id_mapping: Arc<MemObjectIdMapping>,
    /// The log that is used for writing new quads.
    log: MemLog,
}

impl MemQuadStorage {
    /// Creates a new [MemQuadStorage] with the given `object_id_mapping`.
    pub fn new(object_id_mapping: Arc<MemObjectIdMapping>) -> Self {
        Self {
            object_id_mapping,
            log: MemLog::new(),
        }
    }

    /// Creates a snapshot of this storage.
    pub fn snapshot(&self) -> MemQuadStorageSnapshot {
        let log_snapshot = self.log.snapshot();
        MemQuadStorageSnapshot::new(log_snapshot)
    }
}

#[async_trait]
impl QuadStorage for MemQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        // Only object id encoding is supported.
        QuadStorageEncoding::ObjectId(self.object_id_mapping.encoding())
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        Some(self.object_id_mapping.clone())
    }

    fn planners(&self) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        todo!()
    }

    async fn insert_quads(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        self.log
            .transaction(self.object_id_mapping.as_ref(), |writer| {
                writer.insert_quads(quads.as_ref())
            })
            .await
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        todo!()
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        todo!()
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        todo!()
    }

    async fn clear(&self) -> Result<(), StorageError> {
        todo!()
    }

    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        todo!()
    }

    async fn remove_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        todo!()
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        todo!()
    }

    async fn len(&self) -> Result<usize, StorageError> {
        Ok(self.snapshot().len().await)
    }

    async fn validate(&self) -> Result<(), StorageError> {
        todo!()
    }
}
