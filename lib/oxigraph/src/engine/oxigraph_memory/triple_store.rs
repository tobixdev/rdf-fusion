use crate::engine::arrow::QUAD_TABLE_SCHEMA;
use crate::engine::oxigraph_memory::table_provider::OxigraphMemTable;
use crate::engine::triple_store::TripleStore;
use crate::error::StorageError;
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringBuilder};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{col, lit};
use datafusion::prelude::SessionContext;
use oxrdf::{
    GraphNameRef, LiteralRef, NamedNodeRef, Quad, QuadRef, Subject, SubjectRef, Term, TermRef,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct MemoryTripleStore {
    ctx: SessionContext,
}

impl MemoryTripleStore {
    pub async fn new() -> Result<Self, StorageError> {
        let ctx = SessionContext::new();
        let triples_table = OxigraphMemTable::new();
        ctx.register_table("quads", Arc::new(triples_table))
            .map_err(|e| StorageError::from(e))?;
        Ok(MemoryTripleStore { ctx })
    }
}

#[async_trait]
impl TripleStore for MemoryTripleStore {
    async fn contains(&self, quad: &QuadRef<'_>) -> Result<bool, DataFusionError> {
        let quads = self.ctx.table("quads").await?;
        let count = quads
            .filter(col("graph").eq(lit(scalar_graph(&quad.graph_name))))?
            .filter(col("subject").eq(lit(scalar_subject(&quad.subject))))?
            .filter(col("predicate").eq(lit(scalar_predicate(&quad.predicate))))?
            .filter(col("object").eq(lit(scalar_object(&quad.object))))?
            .count()
            .await?;
        Ok(count > 0)
    }

    async fn len(&self) -> Result<usize, DataFusionError> {
        self.ctx.table("quads").await?.count().await
    }

    async fn load_quads(&self, quads: Vec<Quad>) -> Result<usize, DataFusionError> {
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

fn encode_quads(quads: Vec<Quad>) -> Result<Vec<RecordBatch>, DataFusionError> {
    // Create builders
    let mut graph_builder = StringBuilder::new();
    let mut subject_builder = StringBuilder::new();
    let mut predicate_builder = StringBuilder::new();
    let mut object_builder = StringBuilder::new();

    // Populate builders with data
    for quad in quads {
        graph_builder.append_value(&quad.graph_name.to_string());

        // Subject
        match &quad.subject {
            Subject::NamedNode(nn) => {
                subject_builder.append_value(nn.to_string().as_str());
            }
            Subject::BlankNode(bnode) => {
                subject_builder.append_value(bnode.to_string().as_str());
            }
            Subject::Triple(_) => unimplemented!(),
        };

        predicate_builder.append_value(&quad.predicate.to_string());

        // Object
        match &quad.object {
            Term::NamedNode(nn) => {
                object_builder.append_value(nn.to_string().as_str());
            }
            Term::BlankNode(bnode) => {
                object_builder.append_value(bnode.to_string().as_str());
            }
            Term::Literal(literal) => {
                object_builder.append_value(literal.to_string().as_str());
            }
            Term::Triple(_) => unimplemented!(),
        };
    }

    // Create record batch
    Ok(vec![RecordBatch::try_new(
        QUAD_TABLE_SCHEMA.clone(),
        vec![
            Arc::new(graph_builder.finish()),
            Arc::new(subject_builder.finish()),
            Arc::new(predicate_builder.finish()),
            Arc::new(object_builder.finish()),
        ],
    )?])
}

fn scalar_graph(graph: &GraphNameRef<'_>) -> ScalarValue {
    graph.to_string().into()
}

fn scalar_subject(subject: &SubjectRef<'_>) -> ScalarValue {
    match subject {
        SubjectRef::NamedNode(nn) => nn.to_string().into(),
        SubjectRef::BlankNode(bnode) => bnode.to_string().into(),
        SubjectRef::Triple(_) => unimplemented!(),
    }
}

fn scalar_predicate(predicate: &NamedNodeRef<'_>) -> ScalarValue {
    predicate.to_string().into()
}

fn scalar_object(object: &TermRef<'_>) -> ScalarValue {
    match object {
        TermRef::NamedNode(nn) => nn.to_string().into(),
        TermRef::BlankNode(bnode) => bnode.to_string().into(),
        TermRef::Literal(lit) => scalar_literal(lit),
        TermRef::Triple(_) => unimplemented!(),
    }
}

fn scalar_literal(literal: &LiteralRef<'_>) -> ScalarValue {
    literal.to_string().into()
}
