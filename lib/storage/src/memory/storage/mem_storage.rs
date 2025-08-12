use crate::memory::planner::MemQuadStorePlanner;
use crate::memory::storage::index::IndexSet;
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
use tokio::sync::RwLock;

/// A memory-based quad storage.
pub struct MemQuadStorage {
    /// Holds the mapping between terms and object ids.
    object_id_mapping: Arc<MemObjectIdMapping>,
    /// The index set
    indices: Arc<RwLock<IndexSet>>,
}

impl MemQuadStorage {
    /// Creates a new [MemQuadStorage] with the given `object_id_mapping`.
    pub fn new(object_id_mapping: Arc<MemObjectIdMapping>, batch_size: usize) -> Self {
        Self {
            indices: Arc::new(RwLock::new(IndexSet::new(
                object_id_mapping.encoding(),
                batch_size,
            ))),
            object_id_mapping,
        }
    }

    /// Creates a snapshot of this storage.
    pub async fn snapshot(&self) -> MemQuadStorageSnapshot {
        MemQuadStorageSnapshot::new(
            self.encoding(),
            self.object_id_mapping.clone(),
            Arc::new(self.indices.clone().read_owned().await),
        )
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

    async fn insert(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        self.indices.write().await.insert(quads)
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        self.indices.write().await.remove(quad)
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        todo!()
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
        self.indices.write().await.clear()
    }

    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        self.indices.write().await.clear_graph()
    }

    async fn drop_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        self.indices.write().await.drop_named_graph()
    }

    async fn len(&self) -> Result<usize, StorageError> {
        Ok(self.snapshot().await.len().await)
    }

    async fn optimize(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self) -> Result<(), StorageError> {
        Ok(())
    }
}
