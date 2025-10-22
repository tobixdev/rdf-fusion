use crate::memory::storage::index::quad_index::MemQuadIndex;
use crate::memory::storage::index::quad_index_data::MemRowGroup;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstruction, IndexScanInstructions, IndexScanPredicate,
    IndexScanPredicateSource, IndexSet, MemQuadIndexSetScanIterator,
};
use crate::memory::storage::predicate_pushdown::MemStoragePredicateExpr;
use crate::memory::storage::stream::MemIndexScanStream;
use datafusion::arrow::array::{Array, BooleanArray, UInt32Array};
use datafusion::arrow::compute::kernels::cmp::{eq, gt_eq, lt_eq};
use datafusion::arrow::compute::{and, filter, or};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::metrics::BaselineMetrics;
use itertools::{Itertools, repeat_n};
use rdf_fusion_model::{DFResult, TriplePattern, Variable};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
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
                Vec::new(),
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
        dynamic_filters: Vec<Arc<dyn IndexScanPredicateSource>>,
    ) -> Self {
        Self {
            state: ScanState::CollectRelevantRowGroups(
                IndexRefInSet(index_set, index),
                instructions,
                dynamic_filters,
            ),
        }
    }
}

/// The state of the [MemQuadIndexScanIterator].
enum ScanState<TIndexRef: IndexRef> {
    /// Collecting all relevant [MemRowGroup]s in the index. This will copy a reference to all
    /// arrays and can thus drop the [TIndexRef] once this step is done.
    CollectRelevantRowGroups(
        TIndexRef,
        IndexScanInstructions,
        Vec<Arc<dyn IndexScanPredicateSource>>,
    ),
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
    type Item = DFResult<QuadIndexBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &mut self.state {
                ScanState::CollectRelevantRowGroups(
                    index_ref,
                    instructions,
                    dynamic_filters,
                ) => {
                    let instructions = match combine_instructions_with_dynamic_filters(
                        instructions,
                        dynamic_filters,
                    ) {
                        Ok(instructions) => instructions,
                        Err(err) => return Some(Err(err)),
                    };

                    let index = index_ref.get_index();
                    let index_data = index.data();
                    let pruning_result =
                        index_data.prune_relevant_row_groups(&instructions);

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

                            return Some(Ok(QuadIndexBatch {
                                num_rows: batch_size,
                                columns,
                            }));
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

                            return Some(Ok(QuadIndexBatch {
                                num_rows: selection_vector.true_count(),
                                columns,
                            }));
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

fn combine_instructions_with_dynamic_filters(
    instructions: &IndexScanInstructions,
    dynamic_filters: &Vec<Arc<dyn IndexScanPredicateSource>>,
) -> DFResult<IndexScanInstructions> {
    // Filter out any dynamic filters that are not supported by the index.
    let supported_filters = dynamic_filters
        .iter()
        .flat_map(|s| s.current_predicate().ok())
        .collect_vec();

    if supported_filters.is_empty() {
        return Ok(instructions.clone());
    }

    let mut new_instructions = instructions.clone();
    for predicate in supported_filters {
        new_instructions = new_instructions.apply_filter(&predicate)?;
    }

    Ok(new_instructions)
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
        predicate: &IndexScanPredicate,
    ) -> Option<BooleanArray> {
        match predicate {
            IndexScanPredicate::In(ids) => ids
                .iter()
                .map(|id| {
                    eq(
                        data,
                        &ScalarValue::UInt32(Some(id.as_u32())).to_scalar().unwrap(),
                    )
                    .expect("Array length must match, Data Types match")
                })
                .reduce(|lhs, rhs| or(&lhs, &rhs).expect("Array length must match")),
            IndexScanPredicate::EqualTo(name) => {
                let index = instructions.iter().position(|i| match i {
                    Some(IndexScanInstruction::Scan(var, _)) => var == name,
                    _ => false,
                })?;
                Some(
                    eq(all_data[index].as_ref(), data)
                        .expect("Array length must match, Data Types match"),
                )
            }
            IndexScanPredicate::Between(from, to) => {
                let ge = gt_eq(
                    data,
                    &ScalarValue::UInt32(Some(from.as_u32()))
                        .to_scalar()
                        .expect("UInt32 can be converted to a Scalar"),
                )
                .expect("gt_eq supports UInt32");
                let le = lt_eq(
                    data,
                    &ScalarValue::UInt32(Some(to.as_u32()))
                        .to_scalar()
                        .expect("UInt32 can be converted to a Scalar"),
                )
                .expect("lt_eq supports UInt32");
                Some(and(&ge, &le).expect("Inputs are bools and of same length"))
            }
            IndexScanPredicate::False => {
                Some(repeat_n(Some(false), data.len()).collect())
            }
        }
    }
}

/// Encapsulates the state necessary for executing a pattern scan on a [MemQuadIndex].
///
/// See [PlannedIndexScan].
#[derive(Clone, Debug)]
pub struct PlannedPatternScan {
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
    /// A list of dynamic filters that are applied to the scan.
    dynamic_filters: Vec<Arc<dyn IndexScanPredicateSource>>,
}

impl PlannedPatternScan {
    /// Creates a new [PlannedPatternScan].
    pub fn new(
        schema: SchemaRef,
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
        index: IndexConfiguration,
        instructions: Box<IndexScanInstructions>,
        graph_variable: Option<Variable>,
        pattern: Box<TriplePattern>,
    ) -> Self {
        Self {
            schema,
            index_set,
            index,
            instructions,
            graph_variable,
            pattern,
            dynamic_filters: vec![],
        }
    }

    /// Returns a reference to the graph variable.
    pub fn graph_variable(&self) -> Option<&Variable> {
        self.graph_variable.as_ref()
    }

    /// Returns a reference to the [TriplePattern].
    pub fn pattern(&self) -> &TriplePattern {
        self.pattern.as_ref()
    }

    /// Returns a reference to the [IndexConfiguration] that is used to scan the index.
    pub fn selected_index(&self) -> &IndexConfiguration {
        &self.index
    }

    /// Applies the given `filter` to the scan.
    pub fn apply_filter(self, filter: &MemStoragePredicateExpr) -> DFResult<Self> {
        if let MemStoragePredicateExpr::Dynamic(filter) = filter {
            return Ok(self.with_dynamic_filter(
                Arc::clone(filter) as Arc<dyn IndexScanPredicateSource>
            ));
        }

        let new_instructions = self.instructions.apply_filter(filter)?;
        Ok(Self {
            instructions: Box::new(new_instructions),
            ..self
        })
    }

    /// Chooses the new index to scan based on the current instructions.
    pub fn try_find_better_index(self) -> DFResult<Self> {
        let index = self.index_set.choose_index(&self.instructions);
        Ok(Self { index, ..self })
    }

    /// Executes the pattern scan and return the [SendableRecordBatchStream] that implements the
    /// scan. The resulting stream will be cooperative.
    pub fn create_stream(self, metrics: BaselineMetrics) -> SendableRecordBatchStream {
        let iterator = MemQuadIndexSetScanIterator::new(
            Arc::clone(&self.schema),
            self.index_set,
            self.index,
            self.instructions,
            self.dynamic_filters,
        );
        Box::pin(cooperative(MemIndexScanStream::new(
            self.schema,
            iterator,
            metrics,
        )))
    }

    /// Returns a new [PlannedPatternScan] with the given `filter`.
    fn with_dynamic_filter(
        mut self,
        filter: Arc<dyn IndexScanPredicateSource>,
    ) -> PlannedPatternScan {
        self.dynamic_filters.push(filter);
        self
    }
}

impl Display for PlannedPatternScan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] ", self.index)?;

        if let Some(graph_variable) = &self.graph_variable {
            write!(f, "graph={graph_variable}, ")?;
        }

        let pattern = &self.pattern;
        write!(f, "subject={}", pattern.subject)?;
        write!(f, ", predicate={}", pattern.predicate)?;
        write!(f, ", object={}", pattern.object)?;

        let mut additional_filters = self
            .instructions
            .instructions()
            .iter()
            .filter(|i| i.scan_variable().is_some() && i.predicate().is_some())
            .map(|i| format!("{} {}", i.scan_variable().unwrap(), i.predicate().unwrap()))
            .collect::<Vec<String>>();
        additional_filters.extend(self.dynamic_filters.iter().map(|f| f.to_string()));

        if !additional_filters.is_empty() {
            write!(
                f,
                ", additional_filters=[{}]",
                additional_filters.join(", ")
            )?;
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::encoding::EncodedQuad;
    use crate::memory::object_id::{EncodedGraphObjectId, EncodedObjectId};
    use crate::memory::storage::index::{
        IndexComponents, IndexConfiguration, IndexScanInstruction, IndexScanInstructions,
    };
    use crate::memory::storage::predicate_pushdown::MemStoragePredicateExpr;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_dynamic_filters() {
        // Create an index and insert test data
        let mut index =
            IndexSet::new(ObjectIdEncoding::new(4), 100, &[IndexComponents::GSPO]);
        index
            .insert(&[
                quad(0, 1, 10, 100),
                quad(0, 2, 10, 100),
                quad(0, 2, 10, 200),
                quad(0, 3, 10, 100),
            ])
            .unwrap();
        let index = Arc::new(RwLock::new(index));

        // Create a dynamic filter that starts with "True" (matches everything)
        let dynamic_filter = MockDynamicFilter::new(MemStoragePredicateExpr::Between(
            Arc::from("subject"),
            eid(1),
            eid(2),
        ));

        // Create scan instructions
        let instructions = IndexScanInstructions([
            IndexScanInstruction::traverse_with_predicate(IndexScanPredicate::In(
                BTreeSet::from([eid(0)]),
            )),
            IndexScanInstruction::scan_with_predicate(
                "subject".to_owned(),
                IndexScanPredicate::Between(eid(2), eid(3)),
            ),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::traverse_with_predicate(IndexScanPredicate::In(
                BTreeSet::from([eid(200)]),
            )),
        ]);

        // Create iterator with the dynamic filter
        let mut iterator = MemQuadIndexScanIterator::new_from_index_set(
            Arc::new(index.read_owned().await),
            IndexConfiguration {
                object_id_encoding: ObjectIdEncoding::new(4),
                batch_size: 100,
                components: IndexComponents::GSPO,
            },
            instructions.clone(),
            vec![Arc::clone(&dynamic_filter) as Arc<dyn IndexScanPredicateSource>],
        );

        let batch = iterator.next().unwrap().unwrap();
        assert_eq!(batch.num_rows, 1);
    }

    /// A mock implementation of IndexScanPredicateSource for testing.
    #[derive(Debug)]
    struct MockDynamicFilter {
        predicate: Mutex<MemStoragePredicateExpr>,
    }

    impl MockDynamicFilter {
        fn new(predicate: MemStoragePredicateExpr) -> Arc<Self> {
            Arc::new(Self {
                predicate: Mutex::new(predicate),
            })
        }
    }

    impl Display for MockDynamicFilter {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockDynamicFilter")
        }
    }

    impl IndexScanPredicateSource for MockDynamicFilter {
        fn current_predicate(&self) -> DFResult<MemStoragePredicateExpr> {
            Ok(self.predicate.lock().unwrap().clone())
        }
    }

    fn quad(graph_name: u32, subject: u32, predicate: u32, object: u32) -> EncodedQuad {
        EncodedQuad {
            graph_name: EncodedGraphObjectId(eid(graph_name)),
            subject: eid(subject),
            predicate: eid(predicate),
            object: eid(object),
        }
    }

    fn eid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }
}
