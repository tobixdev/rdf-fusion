use crate::oxigraph_memory::table_provider::OxigraphMemTable;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_graph, encode_scalar_object, encode_scalar_predicate, encode_scalar_subject,
};
use arrow_rdf::encoded::{register_rdf_term_udfs, ENC_AS_NATIVE_BOOLEAN, ENC_DECODE, ENC_EQ};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream};
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::{DataFrame, SessionContext};
use graphfusion_engine::error::StorageError;
use graphfusion_engine::results::QueryResults;
use graphfusion_engine::sparql::error::EvaluationError;
use graphfusion_engine::sparql::{Query, QueryExplanation, QueryOptions};
use graphfusion_engine::TripleStore;
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
        let quads = self.ctx.table(TABLE_QUADS).await?;
        let eq = self.ctx.udf(ENC_EQ)?;
        let as_boolean = self.ctx.udf(ENC_AS_NATIVE_BOOLEAN)?;

        let mut matching = quads;
        if let Some(graph_name) = graph_name {
            matching = matching.filter(as_boolean.call(vec![
                eq.call(vec![col(COL_GRAPH), lit(encode_scalar_graph(graph_name))]),
            ]))?
        }
        if let Some(subject) = subject {
            matching = matching.filter(as_boolean.call(vec![
                eq.call(vec![col(COL_SUBJECT), lit(encode_scalar_subject(subject))]),
            ]))?
        }
        if let Some(predicate) = predicate {
            matching = matching.filter(as_boolean.call(vec![eq.call(vec![
                col(COL_PREDICATE),
                lit(encode_scalar_predicate(predicate)),
            ])]))?
        }
        if let Some(object) = object {
            matching = matching.filter(as_boolean.call(vec![
                eq.call(vec![col(COL_OBJECT), lit(encode_scalar_object(object)?)]),
            ]))?
        }

        Ok(matching)
    }
}

#[async_trait]
impl TripleStore for MemoryTripleStore {
    //
    // Querying
    //

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

    async fn quads_for_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let decode = self.ctx.udf(ENC_DECODE)?;
        let result = self
            .match_pattern(graph_name, subject, predicate, object)
            .await?
            .select(vec![
                decode.call(vec![col(COL_GRAPH)]).alias(COL_GRAPH),
                decode.call(vec![col(COL_SUBJECT)]).alias(COL_SUBJECT),
                decode.call(vec![col(COL_PREDICATE)]).alias(COL_PREDICATE),
                decode.call(vec![col(COL_OBJECT)]).alias(COL_OBJECT),
            ])?
            .execute_stream()
            .await?;
        Ok(result)
    }

    async fn execute_query(
        &self,
        query: Query,
        options: QueryOptions,
    ) -> (
        Result<QueryResults, EvaluationError>,
        Option<QueryExplanation>,
    ) {
        todo!()
    }

    //
    // Loading
    //

    async fn load_quads(&self, quads: Vec<Quad>) -> DFResult<usize> {
        let quads_table_provider = self.ctx.table_provider(TABLE_QUADS).await?;
        let oxigraph_mem = quads_table_provider
            .as_any()
            .downcast_ref::<OxigraphMemTable>()
            .unwrap();
        oxigraph_mem
            .load_quads(quads)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    //
    // Removing
    //

    async fn remove<'a>(&self, quad: QuadRef<'_>) -> DFResult<bool> {
        let quads_table_provider = self.ctx.table_provider(TABLE_QUADS).await?;
        let oxigraph_mem = quads_table_provider
            .as_any()
            .downcast_ref::<OxigraphMemTable>()
            .unwrap();
        oxigraph_mem
            .remove(quad)
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }
}
