use crate::oxigraph_memory::store::{MemoryStorageReader, OxigraphMemoryStorage, QuadIterator};
use datafusion::arrow::datatypes::{Schema, SchemaRef};

use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::encoder::EncodedQuad;
use crate::{AResult, DFResult};
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions};
use datafusion::common::{internal_err, DataFusionError};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use rdf_fusion_encoding::plain_term::PlainTermArrayBuilder;
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_SCHEMA;
use rdf_fusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::TermRef;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct OxigraphMemExec {
    reader: Arc<MemoryStorageReader>,
    properties: PlanProperties,
}

impl OxigraphMemExec {
    pub fn new(storage: &OxigraphMemoryStorage, projection: Option<Vec<usize>>) -> Self {
        let projection = projection.unwrap_or((0..DEFAULT_QUAD_SCHEMA.fields.len()).collect());
        let new_fields: Vec<_> = projection
            .iter()
            .map(|i| Arc::clone(DEFAULT_QUAD_SCHEMA.fields.get(*i).unwrap()))
            .collect();
        let schema = SchemaRef::new(Schema::new(new_fields));

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            reader: Arc::new(storage.snapshot()),
            properties: plan_properties,
        }
    }
}

impl Debug for OxigraphMemExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OxigraphMemExec").finish()
    }
}

impl DisplayAs for OxigraphMemExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "OxigraphMemExec")
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
        Ok(Box::pin(OxigraphMemStream::new(
            &self.reader,
            self.schema(),
            context.session_config().batch_size(),
        )))
    }
}

/// Stream that generates record batches on demand
pub struct OxigraphMemStream {
    schema: SchemaRef,
    batch_size: usize,
    iterator: QuadIterator,
}

impl OxigraphMemStream {
    fn new(storage: &MemoryStorageReader, schema: SchemaRef, batch_size: usize) -> Self {
        let iterator = storage.quads_for_pattern(None, None, None, None);
        Self {
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
        _ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut rb_builder = RdfQuadsRecordBatchBuilder::new(Arc::clone(&self.schema));

        while let Some(quad) = self.iterator.next() {
            rb_builder.encode_quad(&quad);
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
        Arc::clone(&self.schema)
    }
}

#[allow(clippy::struct_excessive_bools)]
struct RdfQuadsRecordBatchBuilder {
    schema: SchemaRef,
    graph: PlainTermArrayBuilder,
    subject: PlainTermArrayBuilder,
    predicate: PlainTermArrayBuilder,
    object: PlainTermArrayBuilder,
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
            graph: PlainTermArrayBuilder::default(),
            subject: PlainTermArrayBuilder::default(),
            predicate: PlainTermArrayBuilder::default(),
            object: PlainTermArrayBuilder::default(),
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

    fn encode_quad(&mut self, quad: &EncodedQuad) {
        if self.project_graph {
            encode_term(&mut self.graph, &quad.graph_name);
        }
        if self.project_subject {
            encode_term(&mut self.subject, &quad.subject);
        }
        if self.project_predicate {
            encode_term(&mut self.predicate, &quad.predicate);
        }
        if self.project_object {
            encode_term(&mut self.object, &quad.object);
        }
        self.count += 1;
    }

    fn finish(self) -> AResult<Option<RecordBatch>> {
        if self.count == 0 {
            return Ok(None);
        }

        let mut fields: Vec<Arc<dyn Array>> = Vec::new();
        if self.project_graph {
            fields.push(Arc::new(self.graph.finish()));
        }
        if self.project_subject {
            fields.push(Arc::new(self.subject.finish()));
        }
        if self.project_predicate {
            fields.push(Arc::new(self.predicate.finish()));
        }
        if self.project_object {
            fields.push(Arc::new(self.object.finish()));
        }

        let options = RecordBatchOptions::default().with_row_count(Some(self.count));
        let record_batch =
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), fields, &options)?;
        Ok(Some(record_batch))
    }
}

fn encode_term(builder: &mut PlainTermArrayBuilder, term: &EncodedTerm) {
    let term_ref = match term {
        EncodedTerm::DefaultGraph => None,
        EncodedTerm::NamedNode(node) => Some(TermRef::NamedNode(node.as_ref())),
        EncodedTerm::BlankNode(node) => Some(TermRef::BlankNode(node.as_ref())),
        EncodedTerm::Literal(node) => Some(TermRef::Literal(node.as_ref())),
    };
    match term_ref {
        None => builder.append_null(),
        Some(term_ref) => builder.append_term(term_ref),
    }
}
