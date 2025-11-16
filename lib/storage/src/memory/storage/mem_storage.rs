use crate::index::{IndexComponents, IndexPermutations};
use crate::memory::MemObjectIdMapping;
use crate::memory::planner::MemQuadStorePlanner;
use crate::memory::storage::quad_index::{MemIndexConfiguration, MemQuadIndex};
use crate::memory::storage::snapshot::MemQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::{ObjectIdEncodingRef, ObjectIdMapping};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_model::DFResult;
use rdf_fusion_model::StorageError;
use rdf_fusion_model::{
    GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A memory-based quad storage.
pub struct MemQuadStorage {
    /// The object id encoding.
    object_id_encoding: ObjectIdEncodingRef,
    /// Holds the mapping between terms and object ids.
    object_id_mapping: Arc<MemObjectIdMapping>,
    /// The index set
    indexes: Arc<RwLock<IndexPermutations<MemQuadIndex>>>,
}

impl MemQuadStorage {
    /// Creates a new [MemQuadStorage] with the given `object_id_encoding`.
    pub fn new(
        object_id_mapping: Arc<MemObjectIdMapping>,
        object_id_encoding: ObjectIdEncodingRef,
        batch_size: usize,
    ) -> Self {
        let components = [
            IndexComponents::GSPO,
            IndexComponents::GPOS,
            IndexComponents::GOSP,
        ];
        let indexes = components
            .iter()
            .map(|components| {
                MemQuadIndex::new(MemIndexConfiguration {
                    object_id_encoding: object_id_encoding.clone(),
                    batch_size,
                    components: *components,
                })
            })
            .collect();
        Self {
            indexes: Arc::new(RwLock::new(IndexPermutations::new(
                HashSet::new(),
                indexes,
            ))),
            object_id_mapping,
            object_id_encoding,
        }
    }

    /// Creates a snapshot of this storage.
    pub async fn snapshot(&self) -> MemQuadStorageSnapshot {
        MemQuadStorageSnapshot::new(
            self.encoding(),
            Arc::clone(&self.object_id_mapping),
            Arc::new(Arc::clone(&self.indexes).read_owned().await),
        )
    }
}

#[async_trait]
impl QuadStorage for MemQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        // Only object id encoding is supported.
        QuadStorageEncoding::ObjectId(Arc::clone(&self.object_id_encoding))
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        Some(Arc::clone(&self.object_id_mapping) as Arc<dyn ObjectIdMapping>)
    }

    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        let snapshot = self.snapshot().await;
        vec![Arc::new(MemQuadStorePlanner::new(snapshot))]
    }

    async fn extend(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        let encoded = quads
            .iter()
            .map(|q| self.object_id_mapping.encode_quad(q.as_ref()))
            .collect::<DFResult<Vec<_>>>()
            .expect("TODO");
        self.indexes.write().await.insert(encoded.as_ref())
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        let encoded = self.object_id_mapping.encode_quad(quad).expect("TODO");
        let count = self.indexes.write().await.remove(&[encoded]);
        Ok(count > 0)
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let encoded = self.object_id_mapping.encode_term_intern(graph_name);
        Ok(self.indexes.write().await.insert_named_graph(encoded))
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
        self.indexes.write().await.clear();
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
        self.indexes.write().await.clear_graph(&encoded.0);
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

        Ok(self.indexes.write().await.drop_named_graph(&encoded))
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
