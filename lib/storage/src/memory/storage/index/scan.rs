use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstructions, IndexSet,
};
use crate::memory::storage::stream::MemIndexScanStream;
use datafusion::arrow::array::{RecordBatch, UInt32Array};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::EmptyRecordBatchStream;
use rdf_fusion_model::{TriplePattern, Variable};
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

pub struct MemQuadIndexScanIterator {
    /// A reference to the index.
    state: ScanState,
    /// The instructions to scan the index.
    instructions: IndexScanInstructions,
}

impl MemQuadIndexScanIterator {
    /// TODO
    pub fn new(
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
        index: IndexConfiguration,
        instructions: IndexScanInstructions,
    ) -> Self {
        Self {
            state: ScanState::CollectRelevantBatches(index_set, index),
            instructions,
        }
    }
}

enum ScanState {
    CollectRelevantBatches(Arc<OwnedRwLockReadGuard<IndexSet>>, IndexConfiguration),
    Scanning([Vec<Arc<UInt32Array>>; 4]),
    Finished,
}

impl Iterator for MemQuadIndexScanIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.state = match &self.state {
                ScanState::CollectRelevantBatches(index_set, index) => {
                    let index = index_set.get_index(&index).expect("Index must exist");
                    let index_columns = index.content();
                    let batches = index_columns
                        .iter()
                        .map(|column| {
                            column
                                .batches()
                                .iter()
                                .map(|batches| batches.clone())
                                .collect()
                        })
                        .collect::<Vec<_>>();
                    let batches =
                        TryInto::<[Vec<Arc<UInt32Array>>; 4]>::try_into(batches).unwrap();

                    ScanState::Scanning(batches)
                }
                ScanState::Scanning(batches) => {
                    todo!()
                }
                ScanState::Finished => {
                    return None;
                }
            }
        }
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
                let iterator =
                    MemQuadIndexScanIterator::new(index_set, index, instructions);
                Box::pin(cooperative(MemIndexScanStream::new(
                    schema, iterator, metrics,
                )))
            }
        }
    }
}
