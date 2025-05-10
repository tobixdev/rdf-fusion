use crate::oxigraph_memory::store::{MemoryStorageReader, OxigraphMemoryStorage, QuadIterator};
use datafusion::arrow::datatypes::{Schema, SchemaRef};

use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::encoder::{EncodedQuad, StrLookup};
use crate::oxigraph_memory::hash::StrHash;
use crate::{AResult, DFResult};
use graphfusion_encoding::value_encoding::{TypedValueArrayBuilder, ENC_QUAD_SCHEMA};
use graphfusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions};
use datafusion::common::{internal_err, DataFusionError};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::Stream;
use graphfusion_model::vocab::xsd;
use graphfusion_model::{
    BlankNode, BlankNodeRef, Date, DateTime, DayTimeDuration, Decimal, Duration, Time,
    YearMonthDuration,
};
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
        let projection = projection.unwrap_or((0..ENC_QUAD_SCHEMA.fields.len()).collect());
        let new_fields: Vec<_> = projection
            .iter()
            .map(|i| Arc::clone(ENC_QUAD_SCHEMA.fields.get(*i).unwrap()))
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
            Arc::clone(&self.reader),
            self.schema(),
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
            RdfQuadsRecordBatchBuilder::new(Arc::clone(&self.storage), Arc::clone(&self.schema));

        while let Some(quad) = self.iterator.next() {
            let result = rb_builder.encode_quad(&quad);
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
        Arc::clone(&self.schema)
    }
}

#[allow(clippy::struct_excessive_bools)]
struct RdfQuadsRecordBatchBuilder {
    reader: Arc<MemoryStorageReader>,
    schema: SchemaRef,
    graph: TypedValueArrayBuilder,
    subject: TypedValueArrayBuilder,
    predicate: TypedValueArrayBuilder,
    object: TypedValueArrayBuilder,
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
            graph: TypedValueArrayBuilder::default(),
            subject: TypedValueArrayBuilder::default(),
            predicate: TypedValueArrayBuilder::default(),
            object: TypedValueArrayBuilder::default(),
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

    fn encode_quad(&mut self, quad: &EncodedQuad) -> AResult<()> {
        if self.project_graph {
            encode_term(&self.reader, &mut self.graph, &quad.graph_name)?;
        }
        if self.project_subject {
            encode_term(&self.reader, &mut self.subject, &quad.subject)?;
        }
        if self.project_predicate {
            encode_term(&self.reader, &mut self.predicate, &quad.predicate)?;
        }
        if self.project_object {
            encode_term(&self.reader, &mut self.object, &quad.object)?;
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
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), fields, &options)?;
        Ok(Some(record_batch))
    }
}

fn encode_term(
    reader: &MemoryStorageReader,
    builder: &mut TypedValueArrayBuilder,
    term: &EncodedTerm,
) -> AResult<()> {
    match term {
        EncodedTerm::DefaultGraph => builder.append_null(),
        EncodedTerm::NamedNode { iri_id } => {
            let string = load_string(reader, iri_id)?;
            builder.append_named_node(&string)
        }
        EncodedTerm::NumericalBlankNode { id } => {
            let id = u128::from_be_bytes(*id);
            builder.append_blank_node(BlankNode::new_from_unique_id(id).as_ref())
        }
        EncodedTerm::SmallBlankNode(value) => {
            builder.append_blank_node(BlankNodeRef::new_unchecked(value.as_str()))
        }
        EncodedTerm::BigBlankNode { id_id } => {
            let string = load_string(reader, id_id)?;
            builder.append_blank_node(BlankNodeRef::new_unchecked(string.as_str()))
        }
        EncodedTerm::SmallStringLiteral(str) => builder.append_string(str, None),
        EncodedTerm::BigStringLiteral { value_id } => {
            let string = load_string(reader, value_id)?;
            builder.append_string(&string, None)
        }
        EncodedTerm::SmallSmallLangStringLiteral { value, language } => {
            builder.append_string(value.as_str(), Some(language.as_str()))
        }
        EncodedTerm::SmallBigLangStringLiteral { value, language_id } => {
            let language = load_string(reader, language_id)?;
            builder.append_string(value, Some(language.as_str()))
        }
        EncodedTerm::BigSmallLangStringLiteral { value_id, language } => {
            let value = load_string(reader, value_id)?;
            builder.append_string(&value, Some(language.as_str()))
        }
        EncodedTerm::BigBigLangStringLiteral {
            value_id,
            language_id,
        } => {
            let value = load_string(reader, value_id)?;
            let language = load_string(reader, language_id)?;
            builder.append_string(&value, Some(language.as_str()))
        }
        EncodedTerm::SmallTypedLiteral { value, datatype_id } => {
            let datatype = load_string(reader, datatype_id)?;
            append_typed_literal(builder, value, &datatype)
        }
        EncodedTerm::IntegerLiteral(integer) => {
            let value = i64::from_be_bytes(integer.to_be_bytes());
            builder.append_integer(value.into())
        }
        EncodedTerm::BigTypedLiteral {
            value_id,
            datatype_id,
        } => {
            let value = load_string(reader, value_id)?;
            let datatype = load_string(reader, datatype_id)?;
            append_typed_literal(builder, &value, &datatype)
        }
        EncodedTerm::BooleanLiteral(v) => builder.append_boolean((*v).into()),
        EncodedTerm::FloatLiteral(v) => builder.append_float(f32::from(*v).into()),
        EncodedTerm::DoubleLiteral(v) => builder.append_double(f64::from(*v).into()),
        EncodedTerm::DecimalLiteral(v) => {
            builder.append_decimal(Decimal::from_be_bytes(v.to_be_bytes()))
        }
        EncodedTerm::DateTimeLiteral(v) => {
            builder.append_date_time(DateTime::from_be_bytes(v.to_be_bytes()))
        }
        EncodedTerm::TimeLiteral(v) => builder.append_time(Time::from_be_bytes(v.to_be_bytes())),
        EncodedTerm::DateLiteral(v) => builder.append_date(Date::from_be_bytes(v.to_be_bytes())),
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
            let duration = Duration::from_be_bytes(v.to_be_bytes());
            builder.append_duration(Some(duration.year_month()), Some(duration.day_time()))
        }
        EncodedTerm::YearMonthDurationLiteral(v) => {
            let duration = YearMonthDuration::from_be_bytes(v.to_be_bytes());
            builder.append_duration(Some(duration), None)
        }
        EncodedTerm::DayTimeDurationLiteral(v) => {
            let duration = DayTimeDuration::from_be_bytes(v.to_be_bytes());
            builder.append_duration(None, Some(duration))
        }
    }
}

fn append_typed_literal(
    builder: &mut TypedValueArrayBuilder,
    value: &str,
    datatype: &str,
) -> AResult<()> {
    // Do type promotion.
    match datatype {
        "http://www.w3.org/2001/XMLSchema#byte"
        | "http://www.w3.org/2001/XMLSchema#short"
        | "http://www.w3.org/2001/XMLSchema#int"
        | "http://www.w3.org/2001/XMLSchema#long"
        | "http://www.w3.org/2001/XMLSchema#unsignedByte"
        | "http://www.w3.org/2001/XMLSchema#unsignedShort"
        | "http://www.w3.org/2001/XMLSchema#unsignedInt"
        | "http://www.w3.org/2001/XMLSchema#unsignedLong"
        | "http://www.w3.org/2001/XMLSchema#positiveInteger"
        | "http://www.w3.org/2001/XMLSchema#negativeInteger"
        | "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
        | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger" => {
            match value.parse() {
                Ok(value) => builder.append_integer(value)?,
                Err(_) => builder.append_null()?,
            }
            return Ok(());
        }
        "http://www.w3.org/2001/XMLSchema#dateTimeStamp" => {
            match value.parse() {
                Ok(value) => builder.append_date_time(value)?,
                Err(_) => builder.append_null()?,
            }
            return Ok(());
        }
        _ => {}
    }

    builder.append_typed_literal(value, datatype)
}

fn load_string(reader: &MemoryStorageReader, str_id: &StrHash) -> DFResult<String> {
    // TODO: use different error
    reader
        .get_str(str_id)
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .ok_or(DataFusionError::Internal(String::from(
            "Could not find string in storage",
        )))
}
