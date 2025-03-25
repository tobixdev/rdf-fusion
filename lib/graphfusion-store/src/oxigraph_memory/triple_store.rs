use crate::oxigraph_memory::table_provider::OxigraphMemTable;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_graph, encode_scalar_object, encode_scalar_predicate, encode_scalar_subject,
};
use arrow_rdf::encoded::{ENC_AS_NATIVE_BOOLEAN, ENC_SAME_TERM};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::logical_expr::{col, lit, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use graphfusion_engine::error::StorageError;
use graphfusion_engine::results::QueryResults;
use graphfusion_engine::sparql::error::EvaluationError;
use graphfusion_engine::sparql::{
    evaluate_query, PathToJoinsRule, Query, QueryExplanation, QueryOptions,
};
use graphfusion_engine::TripleStore;
use oxrdf::{GraphNameRef, NamedNodeRef, Quad, QuadRef, SubjectRef, TermRef};
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryTripleStore {
    ctx: SessionContext,
}

impl MemoryTripleStore {
    pub async fn new() -> Result<Self, StorageError> {
        let state = SessionStateBuilder::new()
            .with_analyzer_rule(Arc::new(PathToJoinsRule::default()))
            .build();
        let ctx = SessionContext::from(state);

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
    ) -> DFResult<LogicalPlan> {
        let quads = self.ctx.table(TABLE_QUADS).await?;

        let mut matching = quads;
        if let Some(graph_name) = graph_name {
            matching = matching.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![
                ENC_SAME_TERM.call(vec![col(COL_GRAPH), lit(encode_scalar_graph(graph_name))]),
            ]))?
        }
        if let Some(subject) = subject {
            matching = matching.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![
                ENC_SAME_TERM.call(vec![col(COL_SUBJECT), lit(encode_scalar_subject(subject))]),
            ]))?
        }
        if let Some(predicate) = predicate {
            matching = matching.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
                col(COL_PREDICATE),
                lit(encode_scalar_predicate(predicate)),
            ])]))?
        }
        if let Some(object) = object {
            matching = matching.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![
                ENC_SAME_TERM.call(vec![col(COL_OBJECT), lit(encode_scalar_object(object)?)]),
            ]))?
        }

        Ok(matching.into_unoptimized_plan())
    }
}

#[async_trait]
impl TripleStore for MemoryTripleStore {
    //
    // Querying
    //

    async fn contains(&self, quad: &QuadRef<'_>) -> DFResult<bool> {
        let pattern_plan = self
            .match_pattern(
                Some(quad.graph_name),
                Some(quad.subject),
                Some(quad.predicate),
                Some(quad.object),
            )
            .await?;
        let count = DataFrame::new(self.ctx.state(), pattern_plan)
            .count()
            .await?;
        Ok(count > 0)
    }

    async fn len(&self) -> DFResult<usize> {
        self.ctx.table(TABLE_QUADS).await?.count().await
    }

    async fn quads_for_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let pattern_plan = self
            .match_pattern(graph_name, subject, predicate, object)
            .await?;
        let result = DataFrame::new(self.ctx.state(), pattern_plan)
            .execute_stream()
            .await?;
        Ok(result)
    }

    async fn execute_query(
        &self,
        query: &Query,
        options: QueryOptions,
    ) -> Result<(QueryResults, Option<QueryExplanation>), EvaluationError> {
        evaluate_query(&self.ctx, query, options).await
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
