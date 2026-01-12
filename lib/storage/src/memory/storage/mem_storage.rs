use crate::index::{EncodedQuad, IndexComponents, IndexPermutations};
use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedObjectId};
use crate::memory::planner::MemQuadStorePlanner;
use crate::memory::storage::quad_index::{MemIndexConfiguration, MemQuadIndex};
use crate::memory::storage::snapshot::MemQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::common::internal_err;
use datafusion::physical_planner::ExtensionPlanner;
use itertools::Itertools;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::{
    ObjectIdEncodingRef, ObjectIdMapping, ObjectIdMappingError, ObjectIdSize,
};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_model::StorageError;
use rdf_fusion_model::{DFResult, TermRef};
use rdf_fusion_model::{
    GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A memory-based quad storage.
pub struct MemQuadStorage {
    /// The object id encoding.
    encoding: ObjectIdEncodingRef,
    /// The index set
    indexes: Arc<RwLock<IndexPermutations<MemQuadIndex>>>,
}

impl MemQuadStorage {
    /// Creates a new [MemQuadStorage] with the given `object_id_encoding`.
    ///
    pub fn try_new(
        object_id_encoding: ObjectIdEncodingRef,
        batch_size: usize,
    ) -> DFResult<Self> {
        if object_id_encoding.object_id_size() != ObjectIdSize::try_from(4).unwrap() {
            return internal_err!("Only object id size 4 is supported for now.");
        }

        let components = [
            IndexComponents::GSPO,
            IndexComponents::GPOS,
            IndexComponents::GOSP,
        ];
        let indexes = components
            .iter()
            .map(|components| {
                MemQuadIndex::new(MemIndexConfiguration {
                    object_id_encoding: Arc::clone(&object_id_encoding),
                    batch_size,
                    components: *components,
                })
            })
            .collect();
        Ok(Self {
            indexes: Arc::new(RwLock::new(IndexPermutations::new(
                HashSet::new(),
                indexes,
            ))),
            encoding: object_id_encoding,
        })
    }

    /// Creates a snapshot of this storage.
    pub async fn snapshot(&self) -> MemQuadStorageSnapshot {
        MemQuadStorageSnapshot::new(
            Arc::clone(&self.encoding),
            Arc::new(Arc::clone(&self.indexes).read_owned().await),
        )
    }

    /// TODO
    async fn encode_quad(
        &self,
        quad: QuadRef<'_>,
    ) -> Result<EncodedQuad<EncodedObjectId>, ObjectIdMappingError> {
        let terms: [TermRef<'_>; 3] =
            [quad.subject.into(), quad.predicate.into(), quad.object];

        let graph_name = match quad.graph_name {
            GraphNameRef::NamedNode(nn) => {
                let oid = self.encoding.mapping().encode_scalar(nn.into())?;
                EncodedObjectId::from_4_byte_slice(oid.as_bytes())
            }
            GraphNameRef::BlankNode(bnode) => {
                let oid = self.encoding.mapping().encode_scalar(bnode.into())?;
                EncodedObjectId::from_4_byte_slice(oid.as_bytes())
            }
            GraphNameRef::DefaultGraph => DEFAULT_GRAPH_ID,
        };

        let terms = terms
            .iter()
            .map(|t| self.encoding.mapping().encode_scalar(*t))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(|oid| EncodedObjectId::from_4_byte_slice(oid.as_bytes()))
            .collect_vec();

        Ok(EncodedQuad {
            graph_name,
            subject: terms[0],
            predicate: terms[1],
            object: terms[2],
        })
    }
}

#[async_trait]
impl QuadStorage for MemQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        // Only object id encoding is supported.
        QuadStorageEncoding::ObjectId(Arc::clone(&self.encoding))
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        Some(Arc::clone(self.encoding.mapping()))
    }

    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        let snapshot = self.snapshot().await;
        vec![Arc::new(MemQuadStorePlanner::new(snapshot))]
    }

    async fn extend(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        let mut encoded = Vec::with_capacity(quads.len());

        for quad in quads {
            let enc_quad = self.encode_quad(quad.as_ref()).await?;
            encoded.push(enc_quad);
        }

        self.indexes.write().await.insert(encoded.as_ref())
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        let encoded = self.encode_quad(quad).await?;
        let count = self.indexes.write().await.remove(&[encoded]);
        Ok(count > 0)
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let object_id = self.encoding.mapping().encode_scalar(graph_name.into())?;

        let encoded = EncodedObjectId::try_from(object_id.as_bytes())
            .expect("Object id size checked in try_new.");
        Ok(self.indexes.write().await.insert_named_graph(encoded))
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        Ok(self.snapshot().await.named_graphs()?)
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        Ok(self.snapshot().await.contains_named_graph(graph_name)?)
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.indexes.write().await.clear();
        Ok(())
    }

    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        let encoded = match graph_name {
            GraphNameRef::NamedNode(nn) => {
                let oid = self.encoding.mapping().encode_scalar(nn.into())?;
                EncodedObjectId::from_4_byte_slice(oid.as_bytes())
            }
            GraphNameRef::BlankNode(bnode) => {
                let oid = self.encoding.mapping().encode_scalar(bnode.into())?;
                EncodedObjectId::from_4_byte_slice(oid.as_bytes())
            }
            GraphNameRef::DefaultGraph => DEFAULT_GRAPH_ID,
        };

        self.indexes.write().await.clear_graph(&encoded);
        Ok(())
    }

    async fn drop_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        let Some(encoded) = self
            .encoding
            .mapping()
            .try_get_object_id(graph_name.into())?
        else {
            return Ok(false);
        };

        let encoded = EncodedObjectId::try_from(encoded.as_bytes())
            .expect("Object id size checked in try_new.");
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
