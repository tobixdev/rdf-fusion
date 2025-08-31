use crate::memory::planner::MemQuadStorePlanner;
use crate::memory::storage::index::{IndexComponents, IndexSet};
use crate::memory::storage::snapshot::MemQuadStorageSnapshot;
use crate::memory::MemObjectIdMapping;
use async_trait::async_trait;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_api::storage::QuadStorage;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_common::DFResult;
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
                &[
                    IndexComponents::GSPO,
                    IndexComponents::GPOS,
                    IndexComponents::GOSP,
                ],
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
        let encoded = quads
            .iter()
            .map(|q| self.object_id_mapping.encode_quad(q.as_ref()))
            .collect::<DFResult<Vec<_>>>()
            .expect("TODO");
        self.indices.write().await.insert(encoded.as_ref())
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        let encoded = self.object_id_mapping.encode_quad(quad).expect("TODO");
        let count = self.indices.write().await.remove(&[encoded]);
        Ok(count > 0)
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let encoded = self.object_id_mapping.encode_term_intern(graph_name);
        Ok(self.indices.write().await.insert_named_graph(encoded))
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        Ok(self.snapshot().await.named_graphs())
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        Ok(self.snapshot().await.contains_named_graph(graph_name))
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.indices.write().await.clear();
        Ok(())
    }

    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        let Some(encoded) = self
            .object_id_mapping
            .try_get_encoded_object_id_from_graph_name(graph_name)
        else {
            return Ok(());
        };
        self.indices.write().await.clear_graph(encoded);
        Ok(())
    }

    async fn drop_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        let Some(encoded) = self
            .object_id_mapping
            .try_get_encoded_object_id_from_term(graph_name)
        else {
            return Ok(false);
        };

        Ok(self.indices.write().await.drop_named_graph(encoded))
    }

    async fn len(&self) -> Result<usize, StorageError> {
        Ok(self.snapshot().await.len())
    }

    async fn optimize(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self) -> Result<(), StorageError> {
        Ok(())
    }
}
