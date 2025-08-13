use crate::memory::storage::index::MemHashIndexIterator;
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::Stream;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingArray;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct MemQuadPatternStream {
    /// The schema of the stream.
    schema: SchemaRef,
    /// Current state of the stream.
    state: MemQuadPatternStreamState,
    /// The metrics of the stream.
    metrics: BaselineMetrics,
}

pub enum MemQuadPatternStreamState {
    /// The stream is emitting the results from the index scan.
    Scan(Box<MemHashIndexIterator>),
    /// The stream is finished.
    Finished,
}

impl MemQuadPatternStream {
    /// Creates a new [MemQuadPatternStream].
    pub fn new(
        schema: SchemaRef,
        iterator: Box<MemHashIndexIterator>,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            schema,
            metrics,
            state: MemQuadPatternStreamState::Scan(iterator),
        }
    }
}

impl Stream for MemQuadPatternStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let timer = metrics.elapsed_compute().timer();
        loop {
            match &mut self.state {
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
                                let name = f.name();
                                batch
                                    .columns
                                    .remove(name)
                                    .unwrap_or_else(|| panic!("Must be bound: {name}"))
                                    .into_array()
                            })
                            .collect();
                        let batch = RecordBatch::try_new_with_options(
                            self.schema.clone(),
                            arrays,
                            &RecordBatchOptions::default()
                                .with_row_count(Some(batch.num_results)),
                        )
                        .map_err(DataFusionError::from)?;

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
