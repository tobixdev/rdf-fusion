use crate::memory::storage::index::quad_index::MemQuadIndex;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstructions, IndexSet,
};
use crate::memory::storage::stream::MemIndexScanStream;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::EmptyRecordBatchStream;
use rdf_fusion_model::{TriplePattern, Variable};
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

pub struct MemQuadIndexScanIterator<'index> {
    /// A reference to the index.
    index: &'index MemQuadIndex,
}

impl Iterator for MemQuadIndexScanIterator<'_> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}

/// TODO
#[derive(Clone, Debug)]
pub enum PlannedPatternScan {
    /// TODO
    Empty(SchemaRef),
    /// TODO
    IndexScan {
        schema: SchemaRef,
        /// Holds a read lock on the index set.
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
        /// Which index to scan.
        index: IndexConfiguration,
        /// The instructions to scan the index.
        instructions: IndexScanInstructions,
        /// The graph variable. Used for printing the query plan.
        graph_variable: Option<Variable>,
        /// The triple pattern. Used for printing the query plan.
        pattern: TriplePattern,
    },
}

impl PlannedPatternScan {
    pub fn create_stream(self, metrics: BaselineMetrics) -> SendableRecordBatchStream {
        match self {
            PlannedPatternScan::Empty(schema) => {
                Box::pin(EmptyRecordBatchStream::new(schema))
            }
            PlannedPatternScan::IndexScan {
                schema,
                index_set,
                index,
                instructions,
                ..
            } => {
                let index = index_set.get_index(&index).expect("");
                let actual_iterator = index.scan_quads(instructions);
                let actual_iterator: MemQuadIndexScanIterator<'static> = unsafe {
                    // SAFETY: The Iterator borrows from the backing data in the index,
                    // which is protected and owned by the Arc<OwnedRwLockReadGuard<IndexContent>>
                    // held in the iterator. This Arc is cloned into the iterator struct and
                    // will be kept alive as long as the iterator exists.
                    std::mem::transmute::<
                        MemQuadIndexScanIterator<'_>,
                        MemQuadIndexScanIterator<'static>,
                    >(actual_iterator)
                };

                let iterator = StaticMemQuadIndexScanIterator {
                    index_set: index_set.clone(),
                    iterator: actual_iterator,
                };

                Box::pin(cooperative(MemIndexScanStream::new(
                    schema, iterator, metrics,
                )))
            }
        }
    }
}

pub struct StaticMemQuadIndexScanIterator {
    index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
    iterator: MemQuadIndexScanIterator<'static>,
}

impl Iterator for StaticMemQuadIndexScanIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}
