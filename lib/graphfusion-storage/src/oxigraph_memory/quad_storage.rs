use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::encoder::Decoder;
use crate::oxigraph_memory::store::OxigraphMemoryStorage;
use crate::oxigraph_memory::table_provider::OxigraphMemTable;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use graphfusion_engine::error::{CorruptionError, StorageError};
use graphfusion_engine::QuadStorage;
use model::{GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef, Term};
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryQuadStorage {
    table_name: String,
    table: Arc<OxigraphMemTable>,
    storage: Arc<OxigraphMemoryStorage>,
}

impl MemoryQuadStorage {
    /// Creates a new empty [MemoryQuadStorage].
    ///
    /// It is intended to pass this storage into a GraphFusion engine.
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        let table = Arc::new(OxigraphMemTable::new());
        let storage = table.storage();
        Self {
            table_name,
            table,
            storage,
        }
    }
}

#[async_trait]
impl QuadStorage for MemoryQuadStorage {
    fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    #[allow(trivial_casts)]
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.table) as Arc<dyn TableProvider>
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
            .map(|et| snapshot.decode_term(&et))
            .map(|dt| {
                dt.and_then(|dt| match dt {
                    Term::NamedNode(nnode) => Ok(NamedOrBlankNode::NamedNode(nnode)),
                    Term::BlankNode(bnode) => Ok(NamedOrBlankNode::BlankNode(bnode)),
                    Term::Literal(_) => Err(StorageError::Corruption(CorruptionError::msg(
                        "Unexpected named node term.",
                    ))),
                })
            })
            .collect::<Result<Vec<NamedOrBlankNode>, _>>()
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let encoded_term = EncodedTerm::from(graph_name);
        Ok(self.storage.snapshot().contains_named_graph(&encoded_term))
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.storage.transaction(|mut t| {
            t.clear();
            Ok(())
        })
    }

    async fn clear_graph<'a>(&self, graph_name: GraphNameRef<'a>) -> Result<(), StorageError> {
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
}
