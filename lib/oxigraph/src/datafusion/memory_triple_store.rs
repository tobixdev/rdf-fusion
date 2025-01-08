use crate::datafusion::triple_store::TripleStore;
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::prelude::SessionContext;
use once_cell::sync::Lazy;
use oxrdf::{
    GraphNameRef, LiteralRef, NamedNodeRef, Quad, QuadRef, Subject, SubjectRef, Term, TermRef,
};
use std::sync::Arc;

static GRAPH_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
static SUBJECT_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
static PREDICATE_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
static OBJECT_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);

static SINGLE_QUAD_TABLE_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("graph", GRAPH_TYPE.clone(), false),
        Field::new("subject", SUBJECT_TYPE.clone(), false),
        Field::new("predicate", PREDICATE_TYPE.clone(), false),
        Field::new("object", OBJECT_TYPE.clone(), false),
    ])
});

pub struct MemoryTripleStore {
    ctx: SessionContext,
}

impl MemoryTripleStore {
    pub async fn new() -> Result<Box<Self>, DataFusionError> {
        let ctx = SessionContext::new();
        let triples_table = MemTable::try_new(
            SchemaRef::new(SINGLE_QUAD_TABLE_SCHEMA.clone()),
            vec![Vec::new()],
        )?;
        ctx.register_table("quads", Arc::new(triples_table))?;
        Ok(Box::new(MemoryTripleStore { ctx }))
    }
}

#[async_trait]
impl TripleStore for MemoryTripleStore {
    async fn load_from_reader(&self, quads: Vec<Quad>) -> Result<(), DataFusionError> {
        let schema = SchemaRef::new(SINGLE_QUAD_TABLE_SCHEMA.clone());

        let batches = encode_quads(quads)?;
        let state = self.ctx.state();
        let source = MemoryExec::try_new(&[batches], schema, None)?;
        let physical_plan = self
            .ctx
            .table_provider("quads")
            .await?
            .insert_into(&state, Arc::new(source), InsertOp::Append)
            .await?;

        collect(physical_plan, self.ctx.task_ctx()).await?;

        Ok(())
    }

    async fn contains(&self, quad: &QuadRef<'_>) -> Result<bool, DataFusionError> {
        // TODO: Check others with scalar_value
        let count = self
            .ctx
            .table("quads")
            .await?
            .filter(col("graph").eq(lit(scalar_graph(&quad.graph_name))))?
            .filter(col("subject").eq(lit(scalar_subject(&quad.subject))))?
            .filter(col("predicate").eq(lit(scalar_predicate(&quad.predicate))))?
            .filter(col("object").eq(lit(scalar_object(&quad.object))))?
            .count()
            .await?;
        Ok(count > 0)
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
                subject_builder.append_value(nn.to_string());
            }
            Subject::BlankNode(bnode) => {
                subject_builder.append_value(bnode.to_string());
            }
            Subject::Triple(_) => unimplemented!(),
        };

        predicate_builder.append_value(&quad.predicate.to_string());

        // Object
        match &quad.object {
            Term::NamedNode(nn) => {
                object_builder.append_value(nn.to_string());
            }
            Term::BlankNode(bnode) => {
                object_builder.append_value(bnode.to_string());
            }
            Term::Literal(literal) => {
                object_builder.append_value(literal.to_string());
            }
            Term::Triple(_) => unimplemented!(),
        };
    }

    // Create record batch
    Ok(vec![RecordBatch::try_new(
        SchemaRef::new(SINGLE_QUAD_TABLE_SCHEMA.clone()),
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
