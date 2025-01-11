use crate::engine::oxigraph_memory::table_provider::OxigraphMemTable;
use crate::engine::triple_store::TripleStore;
use crate::engine::DFResult;
use crate::error::StorageError;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use oxrdf::{Quad, QuadRef};
use querymodel::encoded::register_rdf_term_udfs;
use querymodel::encoded::scalars::{
    encode_scalar_graph, encode_scalar_object, encode_scalar_predicate, encode_scalar_subject,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryTripleStore {
    ctx: SessionContext,
}

impl MemoryTripleStore {
    pub async fn new() -> Result<Self, StorageError> {
        let ctx = SessionContext::new();
        register_rdf_term_udfs(&ctx);

        let triples_table = OxigraphMemTable::new();
        ctx.register_table("quads", Arc::new(triples_table))
            .map_err(|e| StorageError::from(e))?;
        Ok(MemoryTripleStore { ctx })
    }
}

#[async_trait]
impl TripleStore for MemoryTripleStore {
    async fn contains(&self, quad: &QuadRef<'_>) -> DFResult<bool> {
        let quads = self.ctx.table("quads").await?;
        let eq = self.ctx.udf("rdf_term_eq")?;
        let as_boolean = self.ctx.udf("rdf_term_as_boolean")?;
        let count = quads
            .filter(as_boolean.call(vec![eq.call(vec![
                col("graph"),
                lit(encode_scalar_graph(quad.graph_name)),
            ])]))?
            .filter(as_boolean.call(vec![eq.call(vec![
                col("subject"),
                lit(encode_scalar_subject(quad.subject)),
            ])]))?
            .filter(as_boolean.call(vec![eq.call(vec![
                col("predicate"),
                lit(encode_scalar_predicate(quad.predicate)),
            ])]))?
            .filter(as_boolean.call(vec![
                eq.call(vec![col("object"), lit(encode_scalar_object(quad.object)?)]),
            ]))?
            .count()
            .await?;
        Ok(count > 0)
    }

    async fn len(&self) -> DFResult<usize> {
        self.ctx.table("quads").await?.count().await
    }

    async fn load_quads(&self, quads: Vec<Quad>) -> DFResult<usize> {
        let quads_table_provider = self.ctx.table_provider("quads").await?;
        let oxigraph_mem = quads_table_provider
            .as_any()
            .downcast_ref::<OxigraphMemTable>()
            .unwrap();
        let len = oxigraph_mem
            .load_quads(quads)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(len)
    }
}
