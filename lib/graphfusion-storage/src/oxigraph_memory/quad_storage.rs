use crate::oxigraph_memory::table_provider::OxigraphMemTable;
use crate::DFResult;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use graphfusion_engine::QuadStorage;
use model::{Quad, QuadRef};
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryQuadStorage {
    table_name: String,
    table: Arc<OxigraphMemTable>,
}

impl MemoryQuadStorage {
    /// Creates a new empty [MemoryQuadStorage].
    ///
    /// It is intended to pass this storage into a GraphFusion engine.
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        let table = Arc::new(OxigraphMemTable::new());
        Self { table_name, table }
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

    async fn load_quads(&self, quads: Vec<Quad>) -> DFResult<usize> {
        self.table
            .load_quads(quads)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn remove<'a>(&self, quad: QuadRef<'_>) -> DFResult<bool> {
        self.table
            .remove(quad)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
