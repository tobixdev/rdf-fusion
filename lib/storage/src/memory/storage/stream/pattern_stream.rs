use crate::memory::encoding::{EncodedActiveGraph, EncodedTriplePattern};
use crate::memory::storage::log::LogChanges;
use crate::memory::storage::stream::quad_equalities::QuadEqualities;
use crate::memory::storage::stream::MemLogInsertionsStream;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::{ready, Stream, StreamExt};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::Variable;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct MemQuadPatternStream {
    /// The schema of the stream.
    schema: SchemaRef,
    /// A stream that reflects the changes in the log.
    log_insertions: Option<Box<MemLogInsertionsStream>>,
    /// Current state of the stream.
    state: MemQuadPatternStreamState,
    /// The metrics of the stream.
    metrics: BaselineMetrics,
}

impl MemQuadPatternStream {
    /// Creates a new [MemQuadPatternStream].
    pub fn new(
        schema: SchemaRef,
        metrics: BaselineMetrics,
        log_insertions: Option<Box<MemLogInsertionsStream>>,
    ) -> Self {
        Self {
            schema,
            metrics,
            log_insertions,
            state: MemQuadPatternStreamState::Start,
        }
    }
}

pub enum MemQuadPatternStreamState {
    /// The stream is waiting for the first poll.
    Start,
    /// The stream is emitting the insertions from the log.
    EmitLogInsertions(Box<MemLogInsertionsStream>),
    /// The stream is finished.
    Finished,
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
                MemQuadPatternStreamState::Start => {
                    let insertions = self.log_insertions.take();

                    match insertions {
                        None => {
                            self.state = MemQuadPatternStreamState::Finished;
                        }
                        Some(stream) => {
                            self.state =
                                MemQuadPatternStreamState::EmitLogInsertions(stream);
                        }
                    }
                }
                MemQuadPatternStreamState::EmitLogInsertions(inner) => {
                    let result = ready!(inner.poll_next_unpin(cx));
                    match result {
                        None => {
                            self.state = MemQuadPatternStreamState::Finished;
                        }
                        Some(changes) => {
                            let batch = changes?;
                            self.metrics.record_output(batch.num_rows());
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }
                }
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
