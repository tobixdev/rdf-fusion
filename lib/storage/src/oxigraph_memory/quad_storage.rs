use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::encoder::EncodedQuad;
use crate::oxigraph_memory::store::{OxigraphMemoryStorage, QuadIterator};
use crate::oxigraph_memory::table_provider::OxigraphMemTable;
use crate::AResult;
use async_trait::async_trait;
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use rdf_fusion_common::error::{CorruptionError, StorageError};
use rdf_fusion_common::{DFResult, QuadPatternEvaluator, QuadStorage};
use rdf_fusion_encoding::plain_term::PlainTermArrayBuilder;
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_SCHEMA;
use rdf_fusion_model::{
    GraphName, GraphNameRef, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
    Subject, Term, TermRef,
};
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub struct MemoryQuadStorage {
    table_name: String,
    table: Arc<OxigraphMemTable>,
    storage: Arc<OxigraphMemoryStorage>,
}

impl MemoryQuadStorage {
    /// Creates a new empty [MemoryQuadStorage].
    ///
    /// It is intended to pass this storage into a RdfFusion engine.
    pub fn new(table_name: impl Into<String>) -> Self {
        let table_name = table_name.into();
        let table = Arc::new(OxigraphMemTable::new());
        let storage = table.storage();
        Self {
            table_name,
            table,
            storage,
        }
    }
}

#[async_trait]
impl QuadStorage for MemoryQuadStorage {
    fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    #[allow(trivial_casts)]
    fn table_provider(&self) -> Arc<dyn TableProvider> {
        Arc::clone(&self.table) as Arc<dyn TableProvider>
    }

    async fn extend(&self, quads: Vec<Quad>) -> Result<usize, StorageError> {
        self.storage.transaction(|mut t| {
            let mut count = 0;
            for quad in &quads {
                let inserted = t.insert(quad.as_ref());
                if inserted {
                    count += 1;
                }
            }
            Ok(count)
        })
    }

    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        self.storage
            .transaction(|mut t| Ok(t.insert_named_graph(graph_name)))
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        let snapshot = self.storage.snapshot();
        snapshot
            .named_graphs()
            .map(|dt| match dt {
                EncodedTerm::NamedNode(nnode) => Ok(NamedOrBlankNode::NamedNode(nnode)),
                EncodedTerm::BlankNode(bnode) => Ok(NamedOrBlankNode::BlankNode(bnode)),
                EncodedTerm::Literal(_) | EncodedTerm::DefaultGraph => Err(
                    StorageError::Corruption(CorruptionError::msg("Unexpected named node term.")),
                ),
            })
            .collect::<Result<Vec<NamedOrBlankNode>, _>>()
    }

    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let encoded_term = match graph_name {
            NamedOrBlankNodeRef::NamedNode(node) => EncodedTerm::NamedNode(node.into_owned()),
            NamedOrBlankNodeRef::BlankNode(node) => EncodedTerm::BlankNode(node.into_owned()),
        };
        Ok(self.storage.snapshot().contains_named_graph(&encoded_term))
    }

    async fn clear(&self) -> Result<(), StorageError> {
        self.storage.transaction(|mut t| {
            t.clear();
            Ok(())
        })
    }

    async fn clear_graph<'a>(&self, graph_name: GraphNameRef<'a>) -> Result<(), StorageError> {
        self.storage.transaction(|mut t| {
            t.clear_graph(graph_name);
            Ok(())
        })
    }

    async fn remove_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        self.storage
            .transaction(|mut t| Ok(t.remove_named_graph(graph_name)))
    }

    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError> {
        self.storage.transaction(|mut t| Ok(t.remove(quad)))
    }
}

#[async_trait]
impl QuadPatternEvaluator for MemoryQuadStorage {
    fn quads_for_pattern(
        &self,
        subject: Option<&Subject>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph_name: Option<&GraphName>,
        batch_size: usize,
    ) -> DFResult<SendableRecordBatchStream> {
        let reader = self.storage.snapshot();
        let itearator = reader.quads_for_pattern(
            subject.map(|s| EncodedTerm::from(s.as_ref())).as_ref(),
            predicate.map(|s| EncodedTerm::from(s.as_ref())).as_ref(),
            object.map(|s| EncodedTerm::from(s.as_ref())).as_ref(),
            graph_name.map(|s| EncodedTerm::from(s.as_ref())).as_ref(),
        );
        Ok(Box::pin(QuadIteratorBatchRecordStream::new(
            itearator, batch_size,
        )))
    }
}

/// Stream that generates record batches on demand
pub struct QuadIteratorBatchRecordStream {
    iterator: QuadIterator,
    batch_size: usize,
}

impl QuadIteratorBatchRecordStream {
    /// TODO
    fn new(iterator: QuadIterator, batch_size: usize) -> Self {
        Self {
            batch_size,
            iterator,
        }
    }
}

impl Stream for QuadIteratorBatchRecordStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut rb_builder = RdfQuadsRecordBatchBuilder::new();

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
    fn new() -> Self {
        Self {
            graph: PlainTermArrayBuilder::new(0),
            subject: PlainTermArrayBuilder::new(0),
            predicate: PlainTermArrayBuilder::new(0),
            object: PlainTermArrayBuilder::new(0),
            count: 0,
        }
    }

    fn count(&self) -> usize {
        self.count
    }

    fn encode_quad(&mut self, quad: &EncodedQuad) {
        encode_term(&mut self.graph, &quad.graph_name);
        encode_term(&mut self.subject, &quad.subject);
        encode_term(&mut self.predicate, &quad.predicate);
        encode_term(&mut self.object, &quad.object);
        self.count += 1;
    }

    fn finish(self) -> AResult<Option<RecordBatch>> {
        if self.count == 0 {
            return Ok(None);
        }

        let mut fields: Vec<Arc<dyn Array>> = Vec::new();
        fields.push(Arc::new(self.graph.finish()));
        fields.push(Arc::new(self.subject.finish()));
        fields.push(Arc::new(self.predicate.finish()));
        fields.push(Arc::new(self.object.finish()));

        let options = RecordBatchOptions::default().with_row_count(Some(self.count));
        let record_batch =
            RecordBatch::try_new_with_options(Arc::clone(&DEFAULT_QUAD_SCHEMA), fields, &options)?;
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
