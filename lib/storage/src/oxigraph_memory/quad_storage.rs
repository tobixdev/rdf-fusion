use crate::oxigraph_memory::planner::OxigraphMemoryQuadNodePlanner;
use crate::oxigraph_memory::store::{MemoryStorageReader, OxigraphMemoryStorage};
use async_trait::async_trait;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_api::storage::QuadStorage;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::{
    GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct MemoryQuadStorage {
    storage: Arc<OxigraphMemoryStorage>,
}

impl MemoryQuadStorage {
    /// Creates a new empty [MemoryQuadStorage].
    ///
    /// It is intended to pass this storage into a RDF Fusion engine.
    pub fn new() -> Self {
        let storage = Arc::new(OxigraphMemoryStorage::new());
        Self { storage }
    }

    /// Creates a read-only snapshot of the current storage state.
    ///
    /// This method returns a read-only view of the storage at the current point in time.
    /// The snapshot can be used to query the storage without being affected by concurrent
    /// modifications.
    /// This is useful for consistent reads across multiple operations.
    ///
    /// # Returns
    /// A read-only snapshot of the storage
    pub fn snapshot(&self) -> MemoryStorageReader {
        self.storage.snapshot()
    }
}

impl Default for MemoryQuadStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuadStorage for MemoryQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        self.storage.storage_encoding()
    }

    async fn extend(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        self.storage.transaction(|mut t| {
            let mut count = 0;
            for quad in &quads {
                let inserted = t.insert(quad.as_ref());
                if inserted {
                    count += 1;
                }
            }
            Ok(count)
        })
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        self.storage
            .transaction(|mut t| Ok(t.insert_named_graph(graph_name)))
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        let snapshot = self.storage.snapshot();
        snapshot
            .named_graphs()
            .map(|object_id| {
                self.storage
                    .object_ids()
                    .try_decode::<NamedOrBlankNode>(object_id)
            })
            .collect::<Result<Vec<NamedOrBlankNode>, _>>()
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let object_id = self.storage.object_ids().try_get_object_id(graph_name);
        match object_id {
            None => Ok(false),
            Some(object_id) => {
                Ok(self.storage.snapshot().contains_named_graph(object_id))
            }
        }
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.storage.transaction(|mut t| {
            t.clear();
            Ok(())
        })
    }

    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        self.storage.transaction(|mut t| {
            t.clear_graph(graph_name);
            Ok(())
        })
    }

    async fn remove_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        self.storage
            .transaction(|mut t| Ok(t.remove_named_graph(graph_name)))
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        self.storage.transaction(|mut t| Ok(t.remove(quad)))
    }

    fn planners(&self) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        vec![Arc::new(OxigraphMemoryQuadNodePlanner::new(self))]
    }

    async fn len(&self) -> Result<usize, StorageError> {
        Ok(self.storage.snapshot().len())
    }
}
