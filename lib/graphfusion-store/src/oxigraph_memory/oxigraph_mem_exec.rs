use crate::oxigraph_memory::store::{MemoryStorageReader, OxigraphMemoryStorage, QuadIterator};
use datafusion::arrow::datatypes::{Schema, SchemaRef};

use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::encoder::{EncodedQuad, StrLookup};
use crate::oxigraph_memory::hash::StrHash;
use crate::{AResult, DFResult};
use arrow_rdf::encoded::{EncRdfTermBuilder, ENC_QUAD_SCHEMA};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions};
use datafusion::common::{internal_err, DataFusionError};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use oxrdf::vocab::xsd;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::io::Write;
use std::sync::Arc;
use std::task::{Context, Poll};
use datamodel::Decimal;

pub struct OxigraphMemExec {
    reader: Arc<MemoryStorageReader>,
    properties: PlanProperties,
}

impl OxigraphMemExec {
    pub fn new(storage: Arc<OxigraphMemoryStorage>, projection: Option<Vec<usize>>) -> Self {
        let projection = projection.unwrap_or((0..ENC_QUAD_SCHEMA.fields.len()).collect());
        let new_fields: Vec<_> = projection
            .iter()
            .map(|i| ENC_QUAD_SCHEMA.fields.get(*i).unwrap().clone())
            .collect();
        let schema = SchemaRef::new(Schema::new(new_fields));

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
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
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "OxigraphMemExec")
            }
        }
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
            self.reader.clone(),
            self.schema().clone(),
            context.session_config().batch_size(),
        )))
    }
}

/// Stream that generates record batches on demand
pub struct OxigraphMemStream {
    storage: Arc<MemoryStorageReader>,
    schema: SchemaRef,
    batch_size: usize,
    iterator: QuadIterator,
}

impl OxigraphMemStream {
    fn new(storage: Arc<MemoryStorageReader>, schema: SchemaRef, batch_size: usize) -> Self {
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
        _ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut rb_builder =
            RdfQuadsRecordBatchBuilder::new(self.storage.clone(), self.schema.clone());

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
    reader: Arc<MemoryStorageReader>,
    schema: SchemaRef,
    graph: EncRdfTermBuilder,
    subject: EncRdfTermBuilder,
    predicate: EncRdfTermBuilder,
    object: EncRdfTermBuilder,
    project_graph: bool,
    project_subject: bool,
    project_predicate: bool,
    project_object: bool,
    count: usize,
}

impl RdfQuadsRecordBatchBuilder {
    fn new(reader: Arc<MemoryStorageReader>, schema: SchemaRef) -> Self {
        let project_graph = schema.column_with_name(COL_GRAPH).is_some();
        let project_subject = schema.column_with_name(COL_SUBJECT).is_some();
        let project_predicate = schema.column_with_name(COL_PREDICATE).is_some();
        let project_object = schema.column_with_name(COL_OBJECT).is_some();
        Self {
            reader,
            schema,
            graph: EncRdfTermBuilder::new(),
            subject: EncRdfTermBuilder::new(),
            predicate: EncRdfTermBuilder::new(),
            object: EncRdfTermBuilder::new(),
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
            encode_term(&self.reader, &mut self.graph, quad.graph_name)?;
        }
        if self.project_subject {
            encode_term(&self.reader, &mut self.subject, quad.subject)?;
        }
        if self.project_predicate {
            encode_term(&self.reader, &mut self.predicate, quad.predicate)?;
        }
        if self.project_object {
            encode_term(&self.reader, &mut self.object, quad.object)?;
        }
        self.count += 1;
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

fn encode_term(
    reader: &MemoryStorageReader,
    builder: &mut EncRdfTermBuilder,
    term: EncodedTerm,
) -> AResult<()> {
    match term {
        EncodedTerm::DefaultGraph => builder.append_null(),
        EncodedTerm::NamedNode { iri_id } => {
            let string = load_string(reader, &iri_id)?;
            builder.append_named_node(&string)
        }
        EncodedTerm::NumericalBlankNode { id } => {
            let id = u128::from_be_bytes(id);
            let mut id_str = [0; 32];
            write!(&mut id_str[..], "{id:032x}")?;
            let value = std::str::from_utf8(id_str.as_ref()).expect("Always valid");
            builder.append_blank_node(value)
        }
        EncodedTerm::SmallBlankNode(value) => builder.append_blank_node(&value),
        EncodedTerm::BigBlankNode { id_id } => {
            let string = load_string(reader, &id_id)?;
            builder.append_blank_node(&string)
        }
        EncodedTerm::SmallStringLiteral(str) => builder.append_string(&str, None),
        EncodedTerm::BigStringLiteral { value_id } => {
            let string = load_string(reader, &value_id)?;
            builder.append_string(&string, None)
        }
        EncodedTerm::SmallSmallLangStringLiteral { value, language } => {
            builder.append_string(value.as_str(), Some(language.as_str()))
        }
        EncodedTerm::SmallBigLangStringLiteral { value, language_id } => {
            let language = load_string(reader, &language_id)?;
            builder.append_string(&value, Some(language.as_str()))
        }
        EncodedTerm::BigSmallLangStringLiteral { value_id, language } => {
            let value = load_string(reader, &value_id)?;
            builder.append_string(&value, Some(language.as_str()))
        }
        EncodedTerm::BigBigLangStringLiteral {
            value_id,
            language_id,
        } => {
            let value = load_string(reader, &value_id)?;
            let language = load_string(reader, &language_id)?;
            builder.append_string(&value, Some(language.as_str()))
        }
        EncodedTerm::SmallTypedLiteral { value, datatype_id } => {
            let datatype = load_string(reader, &datatype_id)?;
            builder.append_typed_literal(&value, &datatype)
        }
        EncodedTerm::IntegerLiteral(integer) => {
            let value = i64::from_be_bytes(integer.to_be_bytes());
            builder.append_integer(value.into())
        }
        EncodedTerm::BigTypedLiteral {
            value_id,
            datatype_id,
        } => {
            let value = load_string(reader, &value_id)?;
            let datatype = load_string(reader, &datatype_id)?;
            builder.append_typed_literal(&value, &datatype)
        }
        EncodedTerm::BooleanLiteral(v) => builder.append_boolean(v.into()),
        EncodedTerm::FloatLiteral(v) => builder.append_float(f32::from(v).into()),
        EncodedTerm::DoubleLiteral(v) => builder.append_double(f64::from(v).into()),
        EncodedTerm::DecimalLiteral(v) => {
            builder.append_decimal(Decimal::from_be_bytes(v.to_be_bytes()))
        }
        EncodedTerm::DateTimeLiteral(v) => {
            // timestamp?
            builder.append_typed_literal(&v.to_string(), xsd::DATE_TIME.as_str())
        }
        EncodedTerm::TimeLiteral(v) => {
            builder.append_typed_literal(&v.to_string(), xsd::TIME.as_str())
        }
        EncodedTerm::DateLiteral(v) => {
            builder.append_typed_literal(&v.to_string(), xsd::DATE.as_str())
        }
        EncodedTerm::GYearMonthLiteral(v) => {
            builder.append_typed_literal(&v.to_string(), xsd::G_YEAR_MONTH.as_str())
        }
        EncodedTerm::GYearLiteral(v) => {
            builder.append_typed_literal(&v.to_string(), xsd::G_YEAR.as_str())
        }
        EncodedTerm::GMonthDayLiteral(v) => {
            builder.append_typed_literal(&v.to_string(), xsd::G_MONTH_DAY.as_str())
        }
        EncodedTerm::GDayLiteral(v) => {
            builder.append_typed_literal(&v.to_string(), xsd::G_DAY.as_str())
        }
        EncodedTerm::GMonthLiteral(v) => {
            builder.append_typed_literal(&v.to_string(), xsd::G_MONTH.as_str())
        }
        EncodedTerm::DurationLiteral(v) => {
            todo!()
        }
        EncodedTerm::YearMonthDurationLiteral(v) => {
            todo!()
        }
        EncodedTerm::DayTimeDurationLiteral(v) => {
            todo!()
        }
        EncodedTerm::Triple(_) => unimplemented!("Encode Triple"),
    }
}

fn load_string(reader: &MemoryStorageReader, str_id: &StrHash) -> DFResult<String> {
    // TODO: use different error
    reader
        .get_str(&str_id)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .ok_or(DataFusionError::Internal(String::from(
            "Could not find string in storage",
        )))
}
