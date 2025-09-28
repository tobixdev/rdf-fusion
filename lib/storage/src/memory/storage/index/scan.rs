use crate::memory::storage::index::quad_index::MemQuadIndex;
use crate::memory::storage::index::quad_index_data::MemRowGroup;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstruction, IndexScanInstructions, IndexSet,
    MemQuadIndexSetScanIterator, ObjectIdScanPredicate,
};
use crate::memory::storage::stream::MemIndexScanStream;
use datafusion::arrow::array::{Array, BooleanArray, UInt32Array};
use datafusion::arrow::compute::kernels::cmp::{eq, neq};
use datafusion::arrow::compute::{and, filter, or};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::metrics::BaselineMetrics;
use rdf_fusion_model::{TriplePattern, Variable};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

/// The results emitted by the [MemQuadIndexScanIterator].
pub struct QuadIndexBatch {
    /// The number of rows in the batch.
    pub num_rows: usize,
    /// A mapping from column name to column data.
    pub columns: HashMap<String, Arc<dyn Array>>,
}

/// Matches a given pattern against a [MemQuadIndex]. The matches are returned as [QuadIndexBatch].
pub struct MemQuadIndexScanIterator<TIndexRef: IndexRef> {
    /// A reference to the index.
    state: ScanState<TIndexRef>,
}

impl<'index> MemQuadIndexScanIterator<DirectIndexRef<'index>> {
    /// Creates a new [MemQuadIndexScanIterator].
    pub fn new(index: &'index MemQuadIndex, instructions: IndexScanInstructions) -> Self {
        Self {
            state: ScanState::CollectRelevantRowGroups(
                DirectIndexRef(index),
                instructions,
            ),
        }
    }
}

impl MemQuadIndexScanIterator<IndexRefInSet> {
    /// Creates a new [MemQuadIndexScanIterator].
    pub fn new_from_index_set(
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
        index: IndexConfiguration,
        instructions: IndexScanInstructions,
    ) -> Self {
        Self {
            state: ScanState::CollectRelevantRowGroups(
                IndexRefInSet(index_set, index),
                instructions,
            ),
        }
    }
}

/// The state of the [MemQuadIndexScanIterator].
enum ScanState<TIndexRef: IndexRef> {
    /// Collecting all relevant [MemRowGroup]s in the index. This will copy a reference to all
    /// arrays and can thus drop the [TIndexRef] once this step is done.
    CollectRelevantRowGroups(TIndexRef, IndexScanInstructions),
    /// Applying the filters and projections to every identified
    Scanning {
        /// The data to scan.
        data: Vec<MemRowGroup>,
        /// Contains the instructions to scan the index. These may differ from the instructions
        /// in [Self::CollectRelevantBatches] if the collecting process already evaluated parts of
        /// the filters. These filters become [None] and ideally, the index should only be scanned
        /// after identifying the batches (we prune the first and last batch if necessary). As a
        /// result, the iterator can simply copy the batches without any more filtering.
        instructions: [Option<IndexScanInstruction>; 4],
    },
    /// The scan is fined.
    Finished,
}

impl<TIndexRef: IndexRef> Iterator for MemQuadIndexScanIterator<TIndexRef> {
    type Item = QuadIndexBatch;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                ScanState::CollectRelevantRowGroups(index_ref, instructions) => {
                    let index = index_ref.get_index();
                    let index_data = index.data();
                    let pruning_result =
                        index_data.prune_relevant_row_groups(instructions);

                    let instructions = match pruning_result.new_instructions {
                        None => instructions.0.clone(),
                        Some(new_instructions) => new_instructions.0,
                    };

                    if pruning_result.row_groups.is_empty() {
                        self.state = ScanState::Finished;
                    } else {
                        self.state = ScanState::Scanning {
                            data: pruning_result.row_groups,
                            instructions: instructions.map(Some),
                        };
                    }
                }
                ScanState::Scanning { data, instructions } => {
                    assert!(!data.is_empty());

                    let next_row_group = data.remove(0);
                    let batch_size = next_row_group.len();
                    let batch = next_row_group.into_arrays();

                    let selection_vector =
                        Self::compute_selection_vector(&batch, instructions);

                    match selection_vector {
                        None => {
                            let columns = batch
                                .iter()
                                .zip(instructions.iter())
                                .filter_map(|(data, instruction)| match instruction {
                                    Some(IndexScanInstruction::Scan(name, _)) => Some((
                                        name.as_str().to_owned(),
                                        Arc::clone(data) as Arc<dyn Array>,
                                    )),
                                    _ => None,
                                })
                                .collect();

                            if data.is_empty() {
                                // This is the last iteration.
                                self.state = ScanState::Finished;
                            }

                            return Some(QuadIndexBatch {
                                num_rows: batch_size,
                                columns,
                            });
                        }
                        Some(selection_vector) => {
                            let columns = batch
                                .iter()
                                .zip(instructions.iter())
                                .filter_map(|(data, instruction)| match instruction {
                                    Some(IndexScanInstruction::Scan(name, _)) => {
                                        Some((name.as_str().to_owned(), data))
                                    }
                                    _ => None,
                                })
                                .map(|(name, data)| {
                                    (
                                        name,
                                        filter(data.as_ref(), &selection_vector)
                                            .expect("Array length must match"),
                                    )
                                })
                                .collect::<HashMap<_, _>>();

                            if data.is_empty() {
                                // This is the last iteration.
                                self.state = ScanState::Finished;
                            }

                            // Don't return empty batches.
                            if selection_vector.true_count() == 0 {
                                continue;
                            }

                            return Some(QuadIndexBatch {
                                num_rows: selection_vector.true_count(),
                                columns,
                            });
                        }
                    }
                }
                ScanState::Finished => {
                    return None;
                }
            }
        }
    }
}

impl<TIndexRef: IndexRef> MemQuadIndexScanIterator<TIndexRef> {
    fn compute_selection_vector(
        data: &[Arc<UInt32Array>; 4],
        instructions: &[Option<IndexScanInstruction>; 4],
    ) -> Option<BooleanArray> {
        data.iter()
            .zip(instructions.iter())
            .filter_map(|(array, instruction)| {
                instruction
                    .as_ref()
                    .and_then(|i| i.predicate())
                    .map(|p| (array, p))
            })
            .flat_map(|(array, predicate)| {
                Self::apply_predicate(data, instructions, array, predicate)
            })
            .reduce(|lhs, rhs| and(&lhs, &rhs).expect("Array length must match"))
    }

    /// Applies the `predicate` to the `data`, returning a boolean array that indicates which
    /// elements match the predicate. This array can then be passed into a filter function.
    ///
    /// If [None] is returned, no predicates need to be applied and the entire data array can be
    /// considered as matching.
    ///
    /// We assume that the number of elements in the sets is relatively small. Therefore, doing
    /// vectorized comparisons and merging the resulting arrays is more performant as iterating
    /// over the array and consulting the set. In the future, one could check the size of the
    /// set and switch to the different strategy for large predicates.
    fn apply_predicate(
        all_data: &[Arc<UInt32Array>; 4],
        instructions: &[Option<IndexScanInstruction>; 4],
        data: &UInt32Array,
        predicate: &ObjectIdScanPredicate,
    ) -> Option<BooleanArray> {
        match predicate {
            ObjectIdScanPredicate::In(ids) => ids
                .iter()
                .map(|id| {
                    eq(
                        data,
                        &ScalarValue::UInt32(Some(id.as_u32())).to_scalar().unwrap(),
                    )
                    .expect("Array length must match, Data Types match")
                })
                .reduce(|lhs, rhs| or(&lhs, &rhs).expect("Array length must match")),
            ObjectIdScanPredicate::Except(ids) => ids
                .iter()
                .map(|id| {
                    neq(
                        data,
                        &ScalarValue::UInt32(Some(id.as_u32())).to_scalar().unwrap(),
                    )
                    .expect("Array length must match, Data Types match")
                })
                .reduce(|lhs, rhs| and(&lhs, &rhs).expect("Array length must match")),
            ObjectIdScanPredicate::EqualTo(name) => {
                let index = instructions.iter().position(|i| match i {
                    Some(IndexScanInstruction::Scan(var, _)) => var == name,
                    _ => false,
                })?;
                Some(
                    eq(all_data[index].as_ref(), data)
                        .expect("Array length must match, Data Types match"),
                )
            }
        }
    }
}

/// Encapsulates the state necessary for executing a pattern scan on a [MemQuadIndex].
#[derive(Clone, Debug)]
pub enum PlannedPatternScan {
    /// It is guaranteed that the scan will return an empty result.
    ///
    /// This can happen if, for example, an object id cannot be found for a bound term.
    Empty(SchemaRef),
    /// A planned pattern scan that can be executed.
    IndexScan {
        /// The result schema.
        schema: SchemaRef,
        /// Holds a read lock on the index set.
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
        /// Which index to scan.
        index: IndexConfiguration,
        /// The instructions to scan the index.
        instructions: Box<IndexScanInstructions>,
        /// The graph variable. Used for printing the query plan.
        graph_variable: Option<Variable>,
        /// The triple pattern. Used for printing the query plan.
        pattern: Box<TriplePattern>,
    },
}

impl PlannedPatternScan {
    /// Checks whether this pattern scan will always return an empty stream.
    pub fn is_guaranteed_empty(&self) -> bool {
        matches!(self, Self::Empty(_))
    }

    /// Executes the pattern scan an return the [SendableRecordBatchStream] that implements the
    /// scan.
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
                let iterator = MemQuadIndexSetScanIterator::new(
                    Arc::clone(&schema),
                    index_set,
                    index,
                    instructions,
                );
                Box::pin(cooperative(MemIndexScanStream::new(
                    schema, iterator, metrics,
                )))
            }
        }
    }
}

/// A reference to a [MemQuadIndex].
pub trait IndexRef {
    /// Returns a reference to the index.
    fn get_index(&self) -> &MemQuadIndex;
}

/// Reference to an index in a locked [IndexSet] with its [IndexConfiguration]. The
/// [IndexConfiguration] uniquely identifier an index within an [IndexSet].
pub struct IndexRefInSet(Arc<OwnedRwLockReadGuard<IndexSet>>, IndexConfiguration);

impl IndexRef for IndexRefInSet {
    fn get_index(&self) -> &MemQuadIndex {
        self.0.find_index(&self.1).expect("Index must exist")
    }
}

/// Directly references a [MemQuadIndex].
pub struct DirectIndexRef<'index>(&'index MemQuadIndex);

impl IndexRef for DirectIndexRef<'_> {
    fn get_index(&self) -> &MemQuadIndex {
        self.0
    }
}
