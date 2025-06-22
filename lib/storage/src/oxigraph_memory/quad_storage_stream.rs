use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::encoder::EncodedQuad;
use crate::oxigraph_memory::store::QuadIterator;
use crate::AResult;
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::DataFusionError;
use datafusion::execution::RecordBatchStream;
use futures::Stream;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::{PlainTermArrayBuilder, PlainTermEncoding};
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_SCHEMA;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{NamedNodePattern, TermPattern, TermRef, TriplePattern, Variable};
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Stream that generates record batches on demand
pub struct QuadPatternBatchRecordStream {
    iterator: QuadIterator,
    graph_variable: Option<Variable>,
    pattern: TriplePattern,
    batch_size: usize,
    equalities: Option<QuadEqualities>,
}

impl QuadPatternBatchRecordStream {
    /// TODO
    pub fn new(
        iterator: QuadIterator,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        batch_size: usize,
    ) -> Self {
        let equalities = QuadEqualities::try_new(graph_variable.as_ref(), &pattern);
        Self {
            iterator,
            graph_variable,
            pattern,
            batch_size,
            equalities,
        }
    }

    /// Creates a builder for the record batches.
    fn create_builder(&self) -> RdfQuadsRecordBatchBuilder {
        let [graph, subject, predicate, object] =
            extract_variables(self.graph_variable.as_ref(), &self.pattern);
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
        let mut buffer: [Option<EncodedQuad>; 32] = [const { None }; 32];
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
        Arc::clone(&DEFAULT_QUAD_SCHEMA)
    }
}

#[allow(clippy::struct_excessive_bools)]
struct RdfQuadsRecordBatchBuilder {
    graph: Option<(Variable, PlainTermArrayBuilder)>,
    subject: Option<(Variable, PlainTermArrayBuilder)>,
    predicate: Option<(Variable, PlainTermArrayBuilder)>,
    object: Option<(Variable, PlainTermArrayBuilder)>,
    count: usize,
}

impl RdfQuadsRecordBatchBuilder {
    fn new(
        graph: Option<Variable>,
        subject: Option<Variable>,
        predicate: Option<Variable>,
        object: Option<Variable>,
        capacity: usize,
    ) -> Self {
        Self {
            graph: graph.map(|v| (v, PlainTermArrayBuilder::new(capacity))),
            subject: subject.map(|v| (v, PlainTermArrayBuilder::new(capacity))),
            predicate: predicate.map(|v| (v, PlainTermArrayBuilder::new(capacity))),
            object: object.map(|v| (v, PlainTermArrayBuilder::new(capacity))),
            count: 0,
        }
    }

    #[allow(
        clippy::expect_used,
        reason = "Checked via count, Maybe use unsafe if performance is an issue"
    )]
    fn encode_batch(&mut self, quads: &[Option<EncodedQuad>; 32]) -> usize {
        let count = quads.iter().position(Option::is_none).unwrap_or(32);

        if let Some((_, builder)) = &mut self.graph {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").graph_name;
                encode_term(builder, value);
            }
        }

        if let Some((_, builder)) = &mut self.subject {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").subject;
                encode_term(builder, value);
            }
        }

        if let Some((_, builder)) = &mut self.predicate {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").predicate;
                encode_term(builder, value);
            }
        }

        if let Some((_, builder)) = &mut self.object {
            for quad in quads.iter().take(count) {
                let value = &quad.as_ref().expect("Checked via count").object;
                encode_term(builder, value);
            }
        }

        self.count += count;
        count
    }

    fn finish(self) -> AResult<RecordBatch> {
        fn try_add_column(
            fields: &mut Vec<Field>,
            arrays: &mut Vec<Arc<dyn Array>>,
            column: Option<(Variable, PlainTermArrayBuilder)>,
            nullable: bool,
        ) {
            if let Some((var, builder)) = column {
                fields.push(Field::new(
                    var.as_str(),
                    PlainTermEncoding::data_type(),
                    nullable,
                ));
                arrays.push(builder.finish());
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
    fn try_new(graph_variable: Option<&Variable>, pattern: &TriplePattern) -> Option<Self> {
        let vars = extract_variables(graph_variable, pattern);

        let mut mapping: HashMap<&Variable, [u8; 4]> = HashMap::new();

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
    fn filter(&self, quads: &mut [Option<EncodedQuad>; 32]) {
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
    fn evaluate(&self, quad: &EncodedQuad) -> bool {
        for equality in &self.0 {
            for i in 0..4 {
                for j in i..4 {
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

/// Returns a buffer of optional variables from `graph_variable` and `pattern`.
#[allow(
    clippy::match_wildcard_for_single_variants,
    reason = "We are only interested in variables"
)]
fn extract_variables(
    graph_variable: Option<&Variable>,
    pattern: &TriplePattern,
) -> [Option<Variable>; 4] {
    [
        graph_variable.cloned(),
        match &pattern.subject {
            TermPattern::Variable(v) => Some(v.clone()),
            _ => None,
        },
        match &pattern.predicate {
            NamedNodePattern::Variable(v) => Some(v.clone()),
            _ => None,
        },
        match &pattern.object {
            TermPattern::Variable(v) => Some(v.clone()),
            _ => None,
        },
    ]
}
