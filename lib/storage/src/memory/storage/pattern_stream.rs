use crate::memory::encoding::{EncodedActiveGraph, EncodedTriplePattern};
use crate::memory::storage::index::{
    IndexScanError, IndexScanInstruction, IndexScanInstructions, IndexSet,
    MemHashIndexIterator,
};
use crate::memory::storage::VersionNumber;
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::{ready, FutureExt, Stream};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_model::Variable;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::OwnedRwLockReadGuard;

pub struct MemQuadPatternStream {
    /// The schema of the stream.
    schema: SchemaRef,
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
    WaitingForIndexScanIterator(
        Pin<
            Box<
                dyn Future<Output = Result<MemHashIndexIterator, IndexScanError>>
                    + Send
                    + Sync,
            >,
        >,
    ),
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
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            schema,
            metrics,
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

                    let index = index_set.choose_index(&scan_instructions);
                    let version_number = *version_number;
                    self.state = MemQuadPatternStreamState::WaitingForIndexScanIterator(
                        Box::pin(async move {
                            index.scan(scan_instructions, version_number).await
                        }),
                    )
                }
                MemQuadPatternStreamState::WaitingForIndexScanIterator(future) => {
                    let index_scan = ready!(future.poll_unpin(cx))
                        .map_err(|e| exec_datafusion_err!("Could not scan index: {e}"))?;
                    self.state = MemQuadPatternStreamState::Scan(index_scan);
                }
                MemQuadPatternStreamState::Scan(inner) => match inner.next() {
                    None => {
                        self.state = MemQuadPatternStreamState::Finished;
                    }
                    Some(mut batch) => {
                        let arrays = self
                            .schema
                            .fields
                            .iter()
                            .map(|f| {
                                batch
                                    .columns
                                    .remove(f.name())
                                    .expect(&format!("Must be bound: {}", f.name()))
                                    .into_array()
                            })
                            .collect();
                        let batch = RecordBatch::try_new_with_options(
                            self.schema.clone(),
                            arrays,
                            &RecordBatchOptions::default()
                                .with_row_count(Some(batch.num_results)),
                        )
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
