use crate::engine::oxigraph_memory::table_provider::OxigraphMemTable;
use crate::engine::triple_store::TripleStore;
use crate::engine::DFResult;
use crate::error::StorageError;
use arrow_rdf::encoded::register_rdf_term_udfs;
use arrow_rdf::encoded::scalars::{
    encode_scalar_graph, encode_scalar_object, encode_scalar_predicate, encode_scalar_subject,
};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream};
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::{DataFrame, SessionContext};
use oxrdf::{GraphNameRef, NamedNodeRef, Quad, QuadRef, SubjectRef, TermRef};
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

    pub async fn match_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<DataFrame> {
        let quads = self.ctx.table("quads").await?;
        let eq = self.ctx.udf("rdf_term_eq")?;
        let as_boolean = self.ctx.udf("rdf_term_as_boolean")?;

        let mut matching = quads;
        if let Some(graph_name) = graph_name {
            matching = matching.filter(as_boolean.call(vec![
                eq.call(vec![col("graph"), lit(encode_scalar_graph(graph_name))]),
            ]))?
        }
        if let Some(subject) = subject {
            matching = matching.filter(as_boolean.call(vec![
                eq.call(vec![col("subject"), lit(encode_scalar_subject(subject))]),
            ]))?
        }
        if let Some(predicate) = predicate {
            matching = matching.filter(as_boolean.call(vec![eq.call(vec![
                col("predicate"),
                lit(encode_scalar_predicate(predicate)),
            ])]))?
        }
        if let Some(object) = object {
            matching = matching.filter(as_boolean.call(vec![
                eq.call(vec![col("object"), lit(encode_scalar_object(object)?)]),
            ]))?
        }

        Ok(matching)
    }
}

#[async_trait]
impl TripleStore for MemoryTripleStore {
    async fn contains(&self, quad: &QuadRef<'_>) -> DFResult<bool> {
        let count = self
            .match_pattern(
                Some(quad.graph_name),
                Some(quad.subject),
                Some(quad.predicate),
                Some(quad.object),
            )
            .await?
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

    async fn quads_for_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<SendableRecordBatchStream> {
        self.match_pattern(graph_name, subject, predicate, object)
            .await?
            .execute_stream()
            .await
    }
}
