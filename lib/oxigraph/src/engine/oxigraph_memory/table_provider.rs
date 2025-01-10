use crate::engine::oxigraph_memory::store::OxigraphMemoryStorage;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;

use crate::engine::oxigraph_memory::oxigraph_mem_exec::OxigraphMemExec;
use crate::error::StorageError;
use datafusion::arrow::array::ArrayBuilder;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use oxrdf::Quad;
use querymodel::encoded::ENC_QUAD_TABLE_SCHEMA;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct OxigraphMemTable {
    storage: Arc<OxigraphMemoryStorage>,
}

impl OxigraphMemTable {
    pub fn new() -> Self {
        let storage = Arc::new(OxigraphMemoryStorage::new());
        Self { storage }
    }

    pub fn load_quads(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        self.storage
            .bulk_loader()
            .load(quads.into_iter().map(Result::<Quad, StorageError>::Ok))
    }
}

impl Debug for OxigraphMemTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OxigraphMemTable").finish()
    }
}

#[async_trait]
impl TableProvider for OxigraphMemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        ENC_QUAD_TABLE_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let exec = OxigraphMemExec::new(self.storage.clone(), projection.cloned());
        Ok(Arc::new(exec))
    }
}
