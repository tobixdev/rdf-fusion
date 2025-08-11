use crate::memory::planner::MemQuadStorePlanner;
use crate::memory::storage::index::IndexSet;
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
    /// The index set
    indices: IndexSet,
}

impl MemQuadStorage {
    /// Creates a new [MemQuadStorage] with the given `object_id_mapping`.
    pub fn new(object_id_mapping: Arc<MemObjectIdMapping>, batch_size: usize) -> Self {
        Self {
            log: MemLog::new(object_id_mapping.clone()),
            indices: IndexSet::new(object_id_mapping.encoding(), batch_size),
            object_id_mapping,
        }
    }

    /// Creates a snapshot of this storage.
    pub async fn snapshot(&self) -> MemQuadStorageSnapshot {
        MemQuadStorageSnapshot::new_with_computed_changes(
            self.encoding(),
            self.object_id_mapping.clone(),
            self.log.snapshot(),
        )
        .await
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

    async fn planners(&self) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        let snapshot = self.snapshot().await;
        vec![Arc::new(MemQuadStorePlanner::new(snapshot))]
    }

    async fn insert_quads(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        self.log
            .transaction(|writer| writer.insert_quads(quads.as_ref()))
            .await
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        self.log
            .transaction(|writer| writer.insert_named_graph(graph_name))
            .await
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        self.snapshot().await.named_graphs().await
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        Ok(self.snapshot().await.contains_named_graph(graph_name).await)
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.log.transaction(|writer| writer.clear()).await
    }

    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        self.log
            .transaction(|writer| writer.clear_graph(graph_name))
            .await
    }

    async fn remove_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        self.log
            .transaction(|writer| writer.drop_named_graph(graph_name))
            .await
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        self.log
            .transaction(|writer| Ok(writer.delete(&[quad])? == 1))
            .await
    }

    async fn len(&self) -> Result<usize, StorageError> {
        Ok(self.snapshot().await.len().await)
    }

    async fn validate(&self) -> Result<(), StorageError> {
        self.log.validate().await
    }
}
