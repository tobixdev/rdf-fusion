use crate::oxigraph_memory::object_id::ObjectIdQuad;
use crate::oxigraph_memory::store::QuadIterator;
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{Column, DataFusionError};
use datafusion::execution::RecordBatchStream;
use futures::Stream;
use rdf_fusion_common::{AResult, BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_model::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::{Context, Poll};

/// Stream that generates record batches on demand
pub struct QuadPatternBatchRecordStream {
    schema: Arc<Schema>,
    iterator: QuadIterator,
    graph_variable: Option<Variable>,
    pattern: TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
    batch_size: usize,
    equalities: Option<QuadEqualities>,
}

impl QuadPatternBatchRecordStream {
    /// Creates a new `QuadPatternBatchRecordStream`.
    ///
    /// # Arguments
    /// * `iterator`: An iterator over quads from the store.
    /// * `graph_variable`: The variable for the graph component, if any.
    /// * `pattern`: The triple pattern to match.
    /// * `blank_node_mode`: How to handle blank nodes.
    /// * `batch_size`: The number of quads to include in each record batch.
    pub fn new(
        iterator: QuadIterator,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        batch_size: usize,
    ) -> Self {
        let schema = Arc::clone(
            compute_schema_for_triple_pattern(
                QuadStorageEncoding::ObjectId,
                graph_variable.as_ref().map(|v| v.as_ref()),
                &pattern,
                blank_node_mode,
            )
            .inner(),
        );
        let equalities =
            QuadEqualities::try_new(graph_variable.as_ref(), &pattern, blank_node_mode);
        Self {
            schema,
            iterator,
            graph_variable,
            pattern,
            blank_node_mode,
            batch_size,
            equalities,
        }
    }

    /// Creates a builder for the record batches.
    fn create_builder(&self) -> RdfQuadsRecordBatchBuilder {
        let [graph, subject, predicate, object] = extract_columns(
            self.graph_variable.as_ref(),
            &self.pattern,
            self.blank_node_mode,
        );
        RdfQuadsRecordBatchBuilder::new(graph, subject, predicate, object, self.batch_size)
    }
}

impl Stream for QuadPatternBatchRecordStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut exhausted = false;
        let mut rb_builder = self.create_builder();

        let mut remaining_items = self.batch_size;
        let mut buffer: [Option<ObjectIdQuad>; 32] = [const { None }; 32];
        while !exhausted && remaining_items > 0 {
            for element in &mut buffer {
                let Some(quad) = self.iterator.next() else {
                    exhausted = true;
                    break;
                };
                *element = Some(quad);
            }

            if let Some(equalities) = &self.equalities {
                equalities.filter(&mut buffer);
            }

            let encoded = rb_builder.encode_batch(&buffer);
            remaining_items -= encoded;

            buffer.fill(None);
        }

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

impl RecordBatchStream for QuadPatternBatchRecordStream {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

#[allow(clippy::struct_excessive_bools)]
struct RdfQuadsRecordBatchBuilder {
    graph: Option<(Column, UInt64Builder)>,
    subject: Option<(Column, UInt64Builder)>,
    predicate: Option<(Column, UInt64Builder)>,
    object: Option<(Column, UInt64Builder)>,
    count: usize,
}

impl RdfQuadsRecordBatchBuilder {
    fn new(
        mut graph: Option<Column>,
        mut subject: Option<Column>,
        mut predicate: Option<Column>,
        mut object: Option<Column>,
        capacity: usize,
    ) -> Self {
        let mut seen = HashSet::new();
        deduplicate(&mut seen, &mut graph);
        deduplicate(&mut seen, &mut subject);
        deduplicate(&mut seen, &mut predicate);
        deduplicate(&mut seen, &mut object);

        Self {
            graph: graph.map(|v| (v, UInt64Builder::with_capacity(capacity))),
            subject: subject.map(|v| (v, UInt64Builder::with_capacity(capacity))),
            predicate: predicate.map(|v| (v, UInt64Builder::with_capacity(capacity))),
            object: object.map(|v| (v, UInt64Builder::with_capacity(capacity))),
            count: 0,
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "Checked via count, Maybe use unsafe if performance is an issue"
    )]
    fn encode_batch(&mut self, quads: &[Option<ObjectIdQuad>; 32]) -> usize {
        let count = quads.iter().position(Option::is_none).unwrap_or(32);

        if let Some((_, builder)) = &mut self.graph {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").graph_name;
                builder.append_value((*value).into())
            }
        }

        if let Some((_, builder)) = &mut self.subject {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").subject;
                builder.append_value((*value).into())
            }
        }

        if let Some((_, builder)) = &mut self.predicate {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").predicate;
                builder.append_value((*value).into())
            }
        }

        if let Some((_, builder)) = &mut self.object {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").object;
                builder.append_value((*value).into())
            }
        }

        self.count += count;
        count
    }

    fn finish(self) -> AResult<RecordBatch> {
        fn try_add_column(
            fields: &mut Vec<Field>,
            arrays: &mut Vec<Arc<dyn Array>>,
            column: Option<(Column, UInt64Builder)>,
            nullable: bool,
        ) {
            if let Some((var, mut builder)) = column {
                fields.push(Field::new(
                    var.name(),
                    DataType::UInt64, // TODO: Use encoding
                    nullable,
                ));
                arrays.push(Arc::new(builder.finish()));
            }
        }

        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

        try_add_column(&mut fields, &mut arrays, self.graph, true);
        try_add_column(&mut fields, &mut arrays, self.subject, false);
        try_add_column(&mut fields, &mut arrays, self.predicate, false);
        try_add_column(&mut fields, &mut arrays, self.object, false);

        let schema = Arc::new(Schema::new(fields));
        let options = RecordBatchOptions::default().with_row_count(Some(self.count));
        let record_batch = RecordBatch::try_new_with_options(schema, arrays, &options)?;
        Ok(record_batch)
    }
}

struct QuadEqualities(Vec<[u8; 4]>);

impl QuadEqualities {
    /// Creates a new [QuadEqualities] fom the variables of the quads.
    fn try_new(
        graph_variable: Option<&Variable>,
        pattern: &TriplePattern,
        blank_node_matching_mode: BlankNodeMatchingMode,
    ) -> Option<Self> {
        let vars = extract_columns(graph_variable, pattern, blank_node_matching_mode);

        let mut mapping: HashMap<&Column, [u8; 4]> = HashMap::new();

        for i in 0..4 {
            if let Some(var) = &vars[i] {
                let value = mapping.entry(var).or_insert([0; 4]);
                value[i] = 1;
            }
        }

        let equalities = mapping
            .into_iter()
            .filter_map(|(_, vars)| {
                let has_equality = vars.into_iter().filter(|v| *v == 1).count() > 1;
                has_equality.then_some(vars)
            })
            .collect::<Vec<_>>();

        if equalities.is_empty() {
            None
        } else {
            Some(Self(equalities))
        }
    }

    /// Filters the buffer in-place and fills up holes by moving all non-None entries to the front.
    /// After this, all `None` slots (holes) will be at the end of the buffer.
    fn filter(&self, quads: &mut [Option<ObjectIdQuad>; 32]) {
        let mut write_idx = 0;

        // Iterate over the buffer and write any matching quad to the write position.
        for read_idx in 0..quads.len() {
            if let Some(quad) = &mut quads[read_idx] {
                if self.evaluate(quad) {
                    if write_idx != read_idx {
                        quads[write_idx] = quads[read_idx].take();
                    }
                    write_idx += 1;
                }
            }
        }

        // Fill the rest with None
        for quad in quads.iter_mut().skip(write_idx) {
            *quad = None;
        }
    }

    /// Evaluates whether the equalities hold for `quad`.
    fn evaluate(&self, quad: &ObjectIdQuad) -> bool {
        for equality in &self.0 {
            for i in 0..4 {
                for j in (i + 1)..4 {
                    if equality[i] == 1 && equality[j] == 1 {
                        let quad = [
                            &quad.graph_name,
                            &quad.subject,
                            &quad.predicate,
                            &quad.object,
                        ];

                        if quad[i] != quad[j] {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }
}

/// Returns a buffer of optional variables from `graph_variable` and `pattern`.
#[allow(
    clippy::match_wildcard_for_single_variants,
    reason = "We are only interested in variables"
)]
fn extract_columns(
    graph_variable: Option<&Variable>,
    pattern: &TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> [Option<Column>; 4] {
    [
        graph_variable
            .cloned()
            .map(|v| Column::new_unqualified(v.as_str())),
        match &pattern.subject {
            TermPattern::Variable(v) => Some(Column::new_unqualified(v.as_str())),
            TermPattern::BlankNode(bnode) if blank_node_mode == BlankNodeMatchingMode::Variable => {
                Some(Column::new_unqualified(bnode.as_str()))
            }
            _ => None,
        },
        match &pattern.predicate {
            NamedNodePattern::Variable(v) => Some(Column::new_unqualified(v.as_str())),
            _ => None,
        },
        match &pattern.object {
            TermPattern::Variable(v) => Some(Column::new_unqualified(v.as_str())),
            TermPattern::BlankNode(bnode) if blank_node_mode == BlankNodeMatchingMode::Variable => {
                Some(Column::new_unqualified(bnode.as_str()))
            }
            _ => None,
        },
    ]
}

fn deduplicate(seen: &mut HashSet<Column>, value: &mut Option<Column>) {
    if let Some(value_taken) = value.take() {
        if !seen.contains(&value_taken) {
            seen.insert(value_taken.clone());
            *value = Some(value_taken);
        }
    }
}
