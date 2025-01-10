use crate::engine::oxigraph_memory::store::{
    MemoryStorageReader, OxigraphMemoryStorage, QuadIterator,
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};

use crate::engine::oxigraph_memory::encoded_term::EncodedTerm;
use crate::engine::oxigraph_memory::encoder::EncodedQuad;
use crate::engine::{AResult, DFResult};
use datafusion::arrow::array::{Array, ArrayBuilder, RecordBatch, RecordBatchOptions};
use datafusion::common::{internal_err, DataFusionError};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use querymodel::encoded::{RdfTermBuilder, ENC_QUAD_TABLE_SCHEMA};
use querymodel::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct OxigraphMemExec {
    storage: Arc<OxigraphMemoryStorage>,
    properties: PlanProperties,
}

impl OxigraphMemExec {
    pub fn new(storage: Arc<OxigraphMemoryStorage>, projection: Option<Vec<usize>>) -> Self {
        let projection = projection.unwrap_or((0..ENC_QUAD_TABLE_SCHEMA.fields.len()).collect());
        let new_fields: Vec<_> = projection
            .iter()
            .map(|i| ENC_QUAD_TABLE_SCHEMA.fields.get(*i).unwrap().clone())
            .collect();
        let schema = SchemaRef::new(Schema::new(new_fields));

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            storage,
            properties: plan_properties,
        }
    }
}

impl Debug for OxigraphMemExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl DisplayAs for OxigraphMemExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for OxigraphMemExec {
    fn name(&self) -> &str {
        "OxigraphMemExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in OxigraphMemExec")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        if partition != 0 {
            return internal_err!("OxigraphMemExec partition must be 1");
        }
        let reader = self.storage.snapshot();
        Ok(Box::pin(OxigraphMemStream::new(
            reader,
            self.schema().clone(),
            context.session_config().batch_size(),
        )))
    }
}

/// Stream that generates record batches on demand
pub struct OxigraphMemStream {
    storage: MemoryStorageReader,
    schema: SchemaRef,
    batch_size: usize,
    iterator: QuadIterator,
}

impl OxigraphMemStream {
    fn new(storage: MemoryStorageReader, schema: SchemaRef, batch_size: usize) -> Self {
        let iterator = storage.quads_for_pattern(None, None, None, None);
        Self {
            storage,
            schema,
            batch_size,
            iterator,
        }
    }
}

impl Stream for OxigraphMemStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut rb_builder = RdfQuadsRecordBatchBuilder::new(self.schema.clone());

        while let Some(quad) = self.iterator.next() {
            let result = rb_builder.encode_quad(quad);
            if let Err(err) = result {
                return Poll::Ready(Some(Err(DataFusionError::External(Box::new(err)))));
            }

            if rb_builder.count() == self.batch_size {
                break;
            }
        }

        let record_batch = rb_builder.finish();
        match record_batch {
            Ok(Some(rb)) => Poll::Ready(Some(Ok(rb))),
            Ok(None) => Poll::Ready(None),
            Err(err) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(err))))),
        }
    }
}

impl RecordBatchStream for OxigraphMemStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct RdfQuadsRecordBatchBuilder {
    schema: SchemaRef,
    graph: RdfTermBuilder,
    subject: RdfTermBuilder,
    predicate: RdfTermBuilder,
    object: RdfTermBuilder,
    project_graph: bool,
    project_subject: bool,
    project_predicate: bool,
    project_object: bool,
    count: usize,
}

impl RdfQuadsRecordBatchBuilder {
    fn new(schema: SchemaRef) -> Self {
        let project_graph = schema.column_with_name(COL_GRAPH).is_some();
        let project_subject = schema.column_with_name(COL_SUBJECT).is_some();
        let project_predicate = schema.column_with_name(COL_PREDICATE).is_some();
        let project_object = schema.column_with_name(COL_OBJECT).is_some();
        Self {
            schema,
            graph: RdfTermBuilder::new(),
            subject: RdfTermBuilder::new(),
            predicate: RdfTermBuilder::new(),
            object: RdfTermBuilder::new(),
            project_graph,
            project_subject,
            project_predicate,
            project_object,
            count: 0,
        }
    }

    fn count(&self) -> usize {
        self.count
    }

    fn encode_quad(&mut self, quad: EncodedQuad) -> AResult<()> {
        if self.project_graph {
            encode_term(&mut self.graph, quad.graph_name)?;
        }
        if self.project_subject {
            encode_term(&mut self.subject, quad.subject)?;
        }
        if self.project_predicate {
            encode_term(&mut self.predicate, quad.predicate)?;
        }
        if self.project_object {
            encode_term(&mut self.object, quad.object)?;
        }
        Ok(())
    }

    fn finish(self) -> AResult<Option<RecordBatch>> {
        if self.count == 0 {
            return Ok(None);
        }

        let mut fields: Vec<Arc<dyn Array>> = Vec::new();
        if self.project_graph {
            fields.push(Arc::new(self.graph.finish()?));
        }
        if self.project_subject {
            fields.push(Arc::new(self.subject.finish()?));
        }
        if self.project_predicate {
            fields.push(Arc::new(self.predicate.finish()?));
        }
        if self.project_object {
            fields.push(Arc::new(self.object.finish()?));
        }

        let options = RecordBatchOptions::default().with_row_count(Some(self.count));
        let record_batch =
            RecordBatch::try_new_with_options(self.schema.clone(), fields, &options)?;
        Ok(Some(record_batch))
    }
}

fn encode_term(builder: &mut RdfTermBuilder, term: EncodedTerm) -> AResult<()> {
    match term {
        EncodedTerm::DefaultGraph => builder.append_small_string("$default"),
        EncodedTerm::NamedNode { iri_id } => builder.append_big_string(&iri_id.to_be_bytes()),
        EncodedTerm::SmallStringLiteral(str) => builder.append_small_string(&str),
        EncodedTerm::BigTypedLiteral {
            value_id,
            datatype_id,
        } => builder.append_big_typed_literal(&value_id.to_be_bytes(), &datatype_id.to_be_bytes()),
        term => unimplemented!("{:?}", term),
    }
}
