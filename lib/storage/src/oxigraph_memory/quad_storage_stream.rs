use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::encoder::EncodedQuad;
use crate::oxigraph_memory::store::QuadIterator;
use crate::AResult;
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::DataFusionError;
use datafusion::execution::RecordBatchStream;
use futures::Stream;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PlainTermArrayBuilder;
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_SCHEMA;
use rdf_fusion_model::TermRef;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Stream that generates record batches on demand
pub struct QuadIteratorBatchRecordStream {
    iterator: QuadIterator,
    batch_size: usize,
}

impl QuadIteratorBatchRecordStream {
    /// TODO
    pub fn new(iterator: QuadIterator, batch_size: usize) -> Self {
        Self {
            iterator,
            batch_size,
        }
    }
}

impl Stream for QuadIteratorBatchRecordStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Early return if the iterator is empty
        let Some(first_quad) = self.iterator.next() else {
            return Poll::Ready(None);
        };

        let mut rb_builder = RdfQuadsRecordBatchBuilder::new(self.batch_size);
        rb_builder.encode_quad(&first_quad);

        for _ in 0..(self.batch_size - 1) {
            let Some(quad) = self.iterator.next() else {
                break;
            };
            rb_builder.encode_quad(&quad);
        }

        let record_batch = rb_builder.finish();
        match record_batch {
            Ok(rb) => Poll::Ready(Some(Ok(rb))),
            Err(err) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(err))))),
        }
    }
}

impl RecordBatchStream for QuadIteratorBatchRecordStream {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&DEFAULT_QUAD_SCHEMA)
    }
}

#[allow(clippy::struct_excessive_bools)]
struct RdfQuadsRecordBatchBuilder {
    graph: PlainTermArrayBuilder,
    subject: PlainTermArrayBuilder,
    predicate: PlainTermArrayBuilder,
    object: PlainTermArrayBuilder,
    count: usize,
}

impl RdfQuadsRecordBatchBuilder {
    fn new(capacity: usize) -> Self {
        Self {
            graph: PlainTermArrayBuilder::new(capacity),
            subject: PlainTermArrayBuilder::new(capacity),
            predicate: PlainTermArrayBuilder::new(capacity),
            object: PlainTermArrayBuilder::new(capacity),
            count: 0,
        }
    }

    fn encode_quad(&mut self, quad: &EncodedQuad) {
        encode_term(&mut self.graph, &quad.graph_name);
        encode_term(&mut self.subject, &quad.subject);
        encode_term(&mut self.predicate, &quad.predicate);
        encode_term(&mut self.object, &quad.object);
        self.count += 1;
    }

    fn finish(self) -> AResult<RecordBatch> {
        let fields: Vec<Arc<dyn Array>> = vec![
            Arc::new(self.graph.finish()),
            Arc::new(self.subject.finish()),
            Arc::new(self.predicate.finish()),
            Arc::new(self.object.finish()),
        ];

        let options = RecordBatchOptions::default().with_row_count(Some(self.count));
        let record_batch =
            RecordBatch::try_new_with_options(Arc::clone(&DEFAULT_QUAD_SCHEMA), fields, &options)?;
        Ok(record_batch)
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
