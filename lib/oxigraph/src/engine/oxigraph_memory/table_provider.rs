use crate::engine::arrow::QUAD_TABLE_SCHEMA;
use crate::engine::oxigraph_memory::store::{
    MemoryStorageReader, OxigraphMemoryStorage, QuadIterator,
};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;

use crate::engine::oxigraph_memory::encoder::Decoder;
use crate::error::StorageError;
use datafusion::arrow::array::{Array, ArrayBuilder, RecordBatch, StringBuilder};
use datafusion::arrow::record_batch::RecordBatchOptions;
use datafusion::error::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    internal_err, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};
use oxrdf::Quad;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::task::{Context, Poll};

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
        QUAD_TABLE_SCHEMA.clone()
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

struct OxigraphMemExec {
    storage: Arc<OxigraphMemoryStorage>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl OxigraphMemExec {
    fn new(storage: Arc<OxigraphMemoryStorage>, projection: Option<Vec<usize>>) -> Self {
        let projection = projection.unwrap_or((0..QUAD_TABLE_SCHEMA.fields.len()).collect());
        let new_fields: Vec<_> = projection
            .iter()
            .map(|i| QUAD_TABLE_SCHEMA.fields.get(*i).unwrap().clone())
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
            schema,
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
            self.schema.clone(),
        )))
    }
}

/// Stream that generates record batches on demand
pub struct OxigraphMemStream {
    storage: MemoryStorageReader,
    schema: SchemaRef,
    iterator: QuadIterator,
}

impl OxigraphMemStream {
    fn new(storage: MemoryStorageReader, schema: SchemaRef) -> Self {
        let iterator = storage.quads_for_pattern(None, None, None, None);
        Self {
            storage,
            schema,
            iterator,
        }
    }
}

impl Stream for OxigraphMemStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut count = 0;
        let mut graph = StringBuilder::new();
        let mut subject = StringBuilder::new();
        let mut predicate = StringBuilder::new();
        let mut object = StringBuilder::new();

        let project_graph = self.schema.column_with_name("graph").is_some();
        let project_subject = self.schema.column_with_name("subject").is_some();
        let project_predicate = self.schema.column_with_name("predicate").is_some();
        let project_object = self.schema.column_with_name("object").is_some();

        while let Some(item) = &self.iterator.next() {
            let quad = self.storage.decode_quad(item);
            match quad {
                Ok(quad) => {
                    if project_graph {
                        graph.append_value(quad.graph_name.to_string());
                    }
                    if project_subject {
                        subject.append_value(quad.subject.to_string());
                    }
                    if project_predicate {
                        predicate.append_value(quad.predicate.to_string());
                    }
                    if project_object {
                        object.append_value(quad.object.to_string());
                    }
                }
                Err(err) => {
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(err)))))
                }
            }
            count += 1;
        }

        if count == 0 {
            return Poll::Ready(None);
        }

        let mut fields: Vec<Arc<dyn Array>> = Vec::new();
        if project_graph {
            fields.push(Arc::new(graph.finish()));
        }
        if project_subject {
            fields.push(Arc::new(subject.finish()));
        }
        if project_predicate {
            fields.push(Arc::new(predicate.finish()));
        }
        if project_object {
            fields.push(Arc::new(object.finish()));
        }

        let options = RecordBatchOptions::default().with_row_count(Some(count));
        let record_batch = RecordBatch::try_new_with_options(self.schema.clone(), fields, &options);
        match record_batch {
            Ok(rb) => Poll::Ready(Some(Ok(rb))),
            Err(err) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(err))))),
        }
    }
}

impl RecordBatchStream for OxigraphMemStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
