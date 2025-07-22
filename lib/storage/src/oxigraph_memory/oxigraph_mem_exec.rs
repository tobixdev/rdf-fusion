use crate::oxigraph_memory::store::{MemoryStorageReader, OxigraphMemoryStorage, QuadIterator};
use datafusion::arrow::datatypes::{Schema, SchemaRef};

use crate::oxigraph_memory::object_id::ObjectIdQuad;
use crate::AResult;
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions, UInt64Builder};
use datafusion::common::{internal_err, DataFusionError};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::PLAIN_TERM_QUAD_SCHEMA;
use rdf_fusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
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
        let projection = projection.unwrap_or((0..PLAIN_TERM_QUAD_SCHEMA.fields.len()).collect());
        let new_fields: Vec<_> = projection
            .iter()
            .map(|i| Arc::clone(PLAIN_TERM_QUAD_SCHEMA.fields.get(*i).unwrap()))
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
    graph: UInt64Builder,
    subject: UInt64Builder,
    predicate: UInt64Builder,
    object: UInt64Builder,
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
            graph: UInt64Builder::default(),
            subject: UInt64Builder::default(),
            predicate: UInt64Builder::default(),
            object: UInt64Builder::default(),
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

    fn encode_quad(&mut self, quad: &ObjectIdQuad) {
        if self.project_graph {
            if quad.graph_name.is_default_graph() {
                self.graph.append_null();
            } else {
                self.graph.append_value(quad.graph_name.into());
            }
        }
        if self.project_subject {
            self.subject.append_value(quad.subject.into());
        }
        if self.project_predicate {
            self.predicate.append_value(quad.predicate.into());
        }
        if self.project_object {
            self.object.append_value(quad.object.into());
        }
        self.count += 1;
    }

    fn finish(mut self) -> AResult<Option<RecordBatch>> {
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
