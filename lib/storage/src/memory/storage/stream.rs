use crate::memory::storage::index::{
    IndexRefInSet, MemQuadIndexScanIterator, MemQuadIndexSetScanIterator,
};
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::RecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use futures::Stream;
use rdf_fusion_common::DFResult;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct MemIndexScanStream {
    /// The schema of the stream.
    schema: SchemaRef,
    /// Current state of the stream.
    iterator: Option<MemQuadIndexSetScanIterator>,
    /// The metrics of the stream.
    metrics: BaselineMetrics,
}

impl MemIndexScanStream {
    /// Creates a new [MemIndexScanStream].
    pub fn new(
        schema: SchemaRef,
        iterator: MemQuadIndexSetScanIterator,
        metrics: BaselineMetrics,
    ) -> Self {
        Self {
            schema,
            iterator: Some(iterator),
            metrics,
        }
    }
}

impl Stream for MemIndexScanStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let metrics = self.metrics.clone();
        let Some(iterator) = &mut self.iterator else {
            return Poll::Ready(None);
        };

        let timer = metrics.elapsed_compute().timer();
        let batch = iterator.next();

        if let Some(batch) = batch {
            let batch = RecordBatch::try_new_with_options(
                self.schema.clone(),
                batch.columns,
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows)),
            )
            .unwrap();
            metrics.record_output(batch.num_rows());
            Poll::Ready(Some(Ok(batch)))
        } else {
            timer.done();
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for MemIndexScanStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
