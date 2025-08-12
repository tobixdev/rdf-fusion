use crate::memory::encoding::{EncodedActiveGraph, EncodedQuad, EncodedTriplePattern};
use crate::memory::storage::stream::quad_filter::QuadFilter;
use crate::memory::storage::stream::{extract_columns, QuadEqualities};
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{exec_err, Column, DataFusionError};
use datafusion::execution::RecordBatchStream;
use futures::Stream;
use rdf_fusion_common::{AResult, BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::{QuadStorageEncoding, TermEncoding};
use rdf_fusion_model::Variable;
use std::cmp::min;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// A stream that emits log insertions.
pub struct MemLogInsertionsStream {
    /// Necessary for creating the record batches.
    storage_encoding: QuadStorageEncoding,
    /// The schema of the stream.
    schema: SchemaRef,
    /// The columns to emit.
    columns: [Option<Column>; 4],
    /// The log changes to emit.
    insertions: Vec<EncodedQuad>,
    /// Filters quads for constants.
    filter: QuadFilter,
    /// Establishes equalities between parts of the pattern (e.g., `?x <...> ?x`)
    equalities: Option<QuadEqualities>,
    /// The batch size.
    batch_size: usize,
    /// The number of already emitted elements.
    consumed: usize,
}

impl MemLogInsertionsStream {
    /// Creates a new log insertion stream.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: SchemaRef,
        storage_encoding: QuadStorageEncoding,
        active_graph: &EncodedActiveGraph,
        graph_variable: Option<&Variable>,
        pattern: &EncodedTriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        insertions: Vec<EncodedQuad>,
        batch_size: usize,
    ) -> Self {
        let columns = extract_columns(graph_variable, pattern, blank_node_mode);
        let filter = QuadFilter::new(&active_graph, pattern);
        let equalities =
            QuadEqualities::try_new(graph_variable, &pattern, blank_node_mode);
        Self {
            storage_encoding,
            schema,
            columns,
            filter,
            insertions,
            equalities,
            batch_size,
            consumed: 0,
        }
    }

    /// Creates a builder for the record batches.
    fn create_builder(&self) -> DFResult<RdfQuadsRecordBatchBuilder> {
        let Some(object_id_encoding) = self.storage_encoding.object_id_encoding() else {
            return exec_err!("Currently only ObjectID encoding is supported.");
        };

        let [graph, subject, predicate, object] = &self.columns;
        RdfQuadsRecordBatchBuilder::new(
            object_id_encoding.clone(),
            graph.clone(),
            subject.clone(),
            predicate.clone(),
            object.clone(),
            self.batch_size,
        )
    }
}

impl Stream for MemLogInsertionsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        assert!(self.consumed <= self.batch_size);

        if self.consumed == self.batch_size {
            return Poll::Ready(None);
        }

        let mut rb_builder = self.create_builder()?;

        let mut consumed = self.consumed;
        let mut remaining_items = self.batch_size;
        let mut buffer: [Option<EncodedQuad>; 32] = [const { None }; 32];
        while consumed < self.insertions.len() && remaining_items > 0 {
            let max_take = min(remaining_items, self.insertions.len() - consumed);
            let take = min(max_take, buffer.len());

            #[allow(clippy::needless_range_loop)]
            for i in 0..take {
                buffer[i] = Some(self.insertions[consumed + i].clone());
            }

            self.filter.remove_non_matching_quads(&mut buffer);
            if let Some(equalities) = &self.equalities {
                equalities.remove_non_matching_quads(&mut buffer);
            }

            let encoded = rb_builder.encode_batch(&buffer)?;
            remaining_items -= encoded;
            consumed += take;

            buffer.fill(None);
        }

        self.consumed = consumed;
        if rb_builder.count == 0 {
            return Poll::Ready(None);
        }

        let record_batch = rb_builder.finish();
        match record_batch {
            Ok(rb) => Poll::Ready(Some(Ok(rb))),
            Err(err) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(err))))),
        }
    }
}

impl RecordBatchStream for MemLogInsertionsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct RdfQuadsRecordBatchBuilder {
    encoding: ObjectIdEncoding,
    graph: Option<(Column, UInt32Builder)>,
    subject: Option<(Column, UInt32Builder)>,
    predicate: Option<(Column, UInt32Builder)>,
    object: Option<(Column, UInt32Builder)>,
    count: usize,
}

impl RdfQuadsRecordBatchBuilder {
    fn new(
        encoding: ObjectIdEncoding,
        mut graph: Option<Column>,
        mut subject: Option<Column>,
        mut predicate: Option<Column>,
        mut object: Option<Column>,
        capacity: usize,
    ) -> DFResult<Self> {
        /// Deduplicates the columns if the same variable is used multiple times.
        fn deduplicate(seen: &mut HashSet<Column>, value: &mut Option<Column>) {
            if let Some(value_taken) = value.take() {
                if !seen.contains(&value_taken) {
                    seen.insert(value_taken.clone());
                    *value = Some(value_taken);
                }
            }
        }

        if encoding.data_type() != DataType::UInt32 {
            return exec_err!("The registered ObjectID uses an unexpected data type.");
        }

        let mut seen = HashSet::new();
        deduplicate(&mut seen, &mut graph);
        deduplicate(&mut seen, &mut subject);
        deduplicate(&mut seen, &mut predicate);
        deduplicate(&mut seen, &mut object);

        Ok(Self {
            encoding,
            graph: graph.map(|v| (v, UInt32Builder::with_capacity(capacity))),
            subject: subject.map(|v| (v, UInt32Builder::with_capacity(capacity))),
            predicate: predicate.map(|v| (v, UInt32Builder::with_capacity(capacity))),
            object: object.map(|v| (v, UInt32Builder::with_capacity(capacity))),
            count: 0,
        })
    }

    #[allow(
        clippy::expect_used,
        reason = "Checked via count, Maybe use unsafe if performance is an issue"
    )]
    fn encode_batch(&mut self, quads: &[Option<EncodedQuad>; 32]) -> AResult<usize> {
        let count = quads.iter().position(Option::is_none).unwrap_or(32);

        if let Some((_, builder)) = &mut self.graph {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").graph_name;
                match value.try_as_encoded_object_id() {
                    None => builder.append_null(),
                    Some(value) => builder.append_value(value.as_object_id().0),
                }
            }
        }

        if let Some((_, builder)) = &mut self.subject {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").subject;
                builder.append_value(value.as_object_id().0)
            }
        }

        if let Some((_, builder)) = &mut self.predicate {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").predicate;
                builder.append_value(value.as_object_id().0)
            }
        }

        if let Some((_, builder)) = &mut self.object {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").object;
                builder.append_value(value.as_object_id().0)
            }
        }

        self.count += count;
        Ok(count)
    }

    fn finish(self) -> AResult<RecordBatch> {
        fn try_add_column(
            encoding: &ObjectIdEncoding,
            fields: &mut Vec<Field>,
            arrays: &mut Vec<Arc<dyn Array>>,
            column: Option<(Column, UInt32Builder)>,
            nullable: bool,
        ) {
            if let Some((var, mut builder)) = column {
                fields.push(Field::new(
                    var.name(),
                    encoding.data_type().clone(),
                    nullable,
                ));
                arrays.push(Arc::new(builder.finish()));
            }
        }

        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        try_add_column(&self.encoding, &mut fields, &mut arrays, self.graph, true);
        try_add_column(
            &self.encoding,
            &mut fields,
            &mut arrays,
            self.subject,
            false,
        );
        try_add_column(
            &self.encoding,
            &mut fields,
            &mut arrays,
            self.predicate,
            false,
        );
        try_add_column(&self.encoding, &mut fields, &mut arrays, self.object, false);

        let schema = Arc::new(Schema::new(fields));
        let options = RecordBatchOptions::default().with_row_count(Some(self.count));
        let record_batch = RecordBatch::try_new_with_options(schema, arrays, &options)?;
        Ok(record_batch)
    }
}
