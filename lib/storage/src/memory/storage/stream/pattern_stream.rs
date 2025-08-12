use crate::memory::encoding::{EncodedActiveGraph, EncodedTriplePattern};
use crate::memory::storage::index::{IndexScanError, IndexScanInstruction, IndexScanInstructions, IndexSet, MemHashIndexIterator};
use crate::memory::storage::stream::QuadEqualities;
use crate::memory::storage::VersionNumber;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::{ready, Stream};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_model::Variable;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::OwnedRwLockReadGuard;

pub struct MemQuadPatternStream {
    /// The schema of the stream.
    schema: SchemaRef,
    /// Quad equalities that must be checked.
    equality: Option<QuadEqualities>,
    /// Current state of the stream.
    state: MemQuadPatternStreamState,
    /// The metrics of the stream.
    metrics: BaselineMetrics,
}

pub enum MemQuadPatternStreamState {
    /// The stream acquires locks.
    CreateIndexScanIterator(
        Arc<OwnedRwLockReadGuard<IndexSet>>,
        VersionNumber,
        EncodedActiveGraph,
        Option<Variable>,
        EncodedTriplePattern,
    ),
    /// Waiting for the index scan iterator to be ready.
    WaitingForIndexScanIterator(Box<dyn Future<Output = Result<MemHashIndexIterator, IndexScanError>>>),
    /// The stream is emitting the results from the index scan.
    Scan(MemHashIndexIterator),
    /// The stream is finished.
    Finished,
}

impl MemQuadPatternStream {
    /// Creates a new [MemQuadPatternStream].
    pub fn new(
        schema: SchemaRef,
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
        version_number: VersionNumber,
        active_graph: EncodedActiveGraph,
        graph_variable: Option<Variable>,
        pattern: EncodedTriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        metrics: BaselineMetrics,
    ) -> Self {
        let equality =
            QuadEqualities::try_new(graph_variable.as_ref(), &pattern, blank_node_mode);
        Self {
            schema,
            metrics,
            equality,
            state: MemQuadPatternStreamState::CreateIndexScanIterator(
                index_set,
                version_number,
                active_graph,
                graph_variable,
                pattern,
            ),
        }
    }
}

impl Stream for MemQuadPatternStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let timer = metrics.elapsed_compute().timer();
        loop {
            match &mut self.state {
                MemQuadPatternStreamState::CreateIndexScanIterator(
                    index_set,
                    version_number,
                    active_graph,
                    graph,
                    pattern,
                ) => {
                    let scan_instructions = IndexScanInstructions([
                        IndexScanInstruction::from_active_graph(
                            active_graph,
                            graph.as_ref(),
                        ),
                        IndexScanInstruction::from(pattern.subject.clone()),
                        IndexScanInstruction::from(pattern.predicate.clone()),
                        IndexScanInstruction::from(pattern.object.clone()),
                    ]);

                    let index_scan = index_set.scan(scan_instructions, *version_number);
                    self.state = MemQuadPatternStreamState::WaitingForIndexScanIterator(
                        Box::new(index_scan),
                    )
                }
                MemQuadPatternStreamState::WaitingForIndexScanIterator(future) => {
                    let index_scan = ready!(future.poll(cx))?;
                    self.state = MemQuadPatternStreamState::Scan(index_scan);
                }
                MemQuadPatternStreamState::Scan(inner) => match inner.next() {
                    None => {
                        self.state = MemQuadPatternStreamState::Finished;
                    }
                    Some(batch) => {
                        let arrow_arrays =
                            batch.into_iter().map(|a| a.into_array()).collect();
                        let batch =
                            RecordBatch::try_new(self.schema.clone(), arrow_arrays)
                                .map_err(|e| DataFusionError::from(e))?;

                        self.metrics.record_output(batch.num_rows());
                        return Poll::Ready(Some(Ok(batch)));
                    }
                },
                MemQuadPatternStreamState::Finished => {
                    timer.done();
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for MemQuadPatternStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
