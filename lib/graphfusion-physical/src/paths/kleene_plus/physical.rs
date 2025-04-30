use crate::DFResult;
use arrow_rdf::as_enc_term_array;
use arrow_rdf::value_encoding::{EncRdfTermBuilder, FromEncodedTerm};
use datafusion::arrow::array::RecordBatchOptions;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, internal_err, plan_err, SchemaExt};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use futures::{Stream, StreamExt};
use graphfusion_logical::paths::PATH_TABLE_SCHEMA;
use model::{GraphName, GraphNameRef, InternalTerm, InternalTermRef};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// Represents a path in the closure.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Path {
    graph: GraphName,
    start: InternalTerm,
    end: InternalTerm,
}

/// Represents a Kleene-plus path closure execution plan. This plan computes the Kleene-plus closure
/// of the inner paths. This closure is the result of the `+` operator in SPARQL property paths.
#[derive(Debug)]
pub struct KleenePlusClosureExec {
    /// The execution properties of this operator.
    plan_properties: PlanProperties,
    /// The inner execution plan.
    inner: Arc<dyn ExecutionPlan>,
    /// See [KleenePlusClosureNode::allow_cross_graph_paths] for details.
    allow_cross_graph_paths: bool,
}

impl KleenePlusClosureExec {
    /// Creates a new [KleenePlusClosureExec] over the `inner` [ExecutionPlan].
    ///
    /// The `allow_cross_graph_paths` argument indicates whether paths are created across multiple
    /// graphs. See [KleenePlusClosureNode::allow_cross_graph_paths] for details.
    pub fn try_new(inner: Arc<dyn ExecutionPlan>, allow_cross_graph_paths: bool) -> DFResult<Self> {
        if !inner
            .schema()
            .equivalent_names_and_types(PATH_TABLE_SCHEMA.as_ref())
        {
            return internal_err!(
                "Invalid schema for KleenePlusPathExec input. Expected: {:?}, got: {:?}",
                PATH_TABLE_SCHEMA.as_ref(),
                inner.schema()
            );
        }

        // Define execution properties
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(inner.schema()),
            Partitioning::UnknownPartitioning(1), // Computation requires all data in one partition
            EmissionType::Final,                  // Emits results only after full computation
            Boundedness::Bounded,                 // Assumes the closure computation terminates
        );
        Ok(Self {
            plan_properties,
            inner,
            allow_cross_graph_paths,
        })
    }
}

impl ExecutionPlan for KleenePlusClosureExec {
    fn name(&self) -> &str {
        "KleenePlusPathExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "KleenePlusPathExec requires exactly one child, got {}",
                children.len()
            );
        }

        let exec =
            KleenePlusClosureExec::try_new(Arc::clone(&children[0]), self.allow_cross_graph_paths)?;
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            // This operator requires a single partition as input.
            return internal_err!(
                "KleenePlusPathExec does not support partitioning (got partition {partition})"
            );
        }

        let partition_count = self.inner.output_partitioning().partition_count();
        let all_partitions = (0..partition_count)
            .map(|i| self.inner.execute(i, Arc::clone(&context)))
            .collect::<DFResult<Vec<_>>>()?;
        let schema = self.schema();
        let input_stream = RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::select_all(all_partitions),
        );

        Ok(Box::pin(KleenePlusClosureStream::new(
            Box::pin(input_stream),
            Arc::clone(&schema),
            self.allow_cross_graph_paths,
        )))
    }
}

impl DisplayAs for KleenePlusClosureExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "KleenePlusPathExec")
    }
}

/// Helper struct to hold the state of our stream
struct KleenePlusClosureStream {
    /// The current state of the stream.
    state: KleenePlusPathStreamState,
    /// The schema of the stream.
    schema: SchemaRef,
    /// See [KleenePlusClosureNode::allow_cross_graph_paths].
    allow_cross_graph_paths: bool,

    // State
    initial_paths_map: HashMap<GraphName, HashSet<(InternalTerm, InternalTerm)>>,
    all_paths: HashSet<Path>,
    current_delta: Vec<Path>,
}

/// Enum to track the state of our stream processing
enum KleenePlusPathStreamState {
    /// Initial state - need to collect input batches
    CollectingInput { stream: SendableRecordBatchStream },
    /// Computing the closure
    Computing,
    /// Done - either yielding the final batch or already finished
    Done,
    /// Error state
    Error,
}

impl KleenePlusClosureStream {
    /// Creates a new [KleenePlusClosureStream].
    ///
    /// See [KleenePlusClosureNode::allow_cross_graph_paths] for details on
    /// `allow_cross_graph_paths`.
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        allow_cross_graph_paths: bool,
    ) -> Self {
        Self {
            state: KleenePlusPathStreamState::CollectingInput { stream: input },
            schema,
            allow_cross_graph_paths,
            initial_paths_map: HashMap::new(),
            all_paths: HashSet::new(),
            current_delta: Vec::new(),
        }
    }

    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<KleenePlusClosureStream as Stream>::Item>> {
        loop {
            match &mut self.state {
                KleenePlusPathStreamState::CollectingInput { stream } => {
                    match ready!(stream.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            if let Err(e) = self.collect_next_batch(&batch) {
                                self.state = KleenePlusPathStreamState::Error;
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        Some(Err(e)) => {
                            self.state = KleenePlusPathStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            self.state = KleenePlusPathStreamState::Computing;
                        }
                    }
                }
                KleenePlusPathStreamState::Computing => {
                    // All input collected, jump to Done state
                    if self.all_paths.is_empty() {
                        self.state = KleenePlusPathStreamState::Done;
                        continue;
                    }

                    // Compute one iteration of the closure
                    let mut next_delta = Vec::new();

                    for current_path in &self.current_delta {
                        if self.allow_cross_graph_paths {
                            Self::compute_new_cross_graph_paths(
                                &self.initial_paths_map,
                                &mut self.all_paths,
                                &mut next_delta,
                                current_path,
                            );
                        } else {
                            Self::compute_new_single_graph_paths(
                                &self.initial_paths_map,
                                &mut self.all_paths,
                                &mut next_delta,
                                &current_path.graph,
                                current_path,
                            );
                        }
                    }

                    if next_delta.is_empty() {
                        // Closure computation complete, create the final batch
                        return match self.create_output_batch() {
                            Ok(batch) => {
                                self.state = KleenePlusPathStreamState::Done;
                                Poll::Ready(Some(Ok(batch)))
                            }
                            Err(e) => {
                                self.state = KleenePlusPathStreamState::Error;
                                Poll::Ready(Some(Err(e)))
                            }
                        };
                    }

                    self.current_delta = next_delta;
                }
                KleenePlusPathStreamState::Done => {
                    return Poll::Ready(None);
                }
                KleenePlusPathStreamState::Error => {
                    return Poll::Ready(Some(Err(exec_datafusion_err!("Error already occurred."))));
                }
            }
        }
    }

    /// Collects the inner paths of a single [RecordBatch].
    ///
    /// This adds all inner paths to the `initial_paths_map`, `all_paths`, and the `current_delta`.
    fn collect_next_batch(&mut self, batch: &RecordBatch) -> DFResult<()> {
        let graph_names = as_enc_term_array(batch.column(0))?;
        let starts = as_enc_term_array(batch.column(1))?;
        let ends = as_enc_term_array(batch.column(2))?;

        for i in 0..batch.num_rows() {
            let graph = GraphNameRef::from_enc_array(graph_names, i).map_err(|_| {
                exec_datafusion_err!("Could not obtain graph value from inner paths.")
            })?;
            let start = InternalTermRef::from_enc_array(starts, i).map_err(|_| {
                exec_datafusion_err!("Could not obtain start value from inner paths.")
            })?;
            let end = InternalTermRef::from_enc_array(ends, i).map_err(|_| {
                exec_datafusion_err!("Could not obtain end value from inner paths.")
            })?;

            let path_tuple = (start.into_owned(), end.into_owned());
            self.initial_paths_map
                .entry(graph.into_owned())
                .or_default()
                .insert(path_tuple);

            let path = Path {
                graph: graph.into_owned(),
                start: start.into_owned(),
                end: end.into_owned(),
            };

            self.all_paths.insert(path.clone()); // All inner paths are part of the closure.
            self.current_delta.push(path); // All inner paths must be part of the next iteration.
        }
        Ok(())
    }

    fn compute_new_cross_graph_paths(
        initial_paths_map: &HashMap<GraphName, HashSet<(InternalTerm, InternalTerm)>>,
        all_paths: &mut HashSet<Path>,
        next_delta: &mut Vec<Path>,
        current_path: &Path,
    ) {
        for graph in initial_paths_map.keys() {
            Self::compute_new_single_graph_paths(
                initial_paths_map,
                all_paths,
                next_delta,
                graph,
                current_path,
            );
        }
    }

    fn compute_new_single_graph_paths(
        initial_paths_map: &HashMap<GraphName, HashSet<(InternalTerm, InternalTerm)>>,
        all_paths: &mut HashSet<Path>,
        next_delta: &mut Vec<Path>,
        graph_name: &GraphName,
        current_path: &Path,
    ) {
        if let Some(initial_paths_from_b) = initial_paths_map.get(graph_name) {
            for (initial_start_b, initial_end_c) in initial_paths_from_b {
                if &current_path.end == initial_start_b {
                    let path_ac = Path {
                        graph: current_path.graph.clone(),
                        start: current_path.start.clone(),
                        end: initial_end_c.clone(),
                    };

                    if all_paths.insert(path_ac.clone()) {
                        next_delta.push(path_ac);
                    }
                }
            }
        }
    }

    /// Creates a [RecordBatch] from the internal state of `self`.
    fn create_output_batch(&self) -> DFResult<RecordBatch> {
        let mut graph_builder = EncRdfTermBuilder::default();
        let mut start_builder = EncRdfTermBuilder::default();
        let mut end_builder = EncRdfTermBuilder::default();

        for path in &self.all_paths {
            match &path.graph {
                GraphName::NamedNode(named) => graph_builder.append_named_node(named.as_str())?,
                GraphName::BlankNode(bnode) => graph_builder.append_blank_node(bnode.as_ref())?,
                GraphName::DefaultGraph => graph_builder.append_null()?,
            }
            start_builder.append_decoded_term(&path.start.as_ref().into_decoded())?;
            end_builder.append_decoded_term(&path.end.as_ref().into_decoded())?;
        }

        let graph_array = graph_builder.finish()?;
        let start_array = start_builder.finish()?;
        let end_array = end_builder.finish()?;

        let options = RecordBatchOptions::new().with_row_count(Some(self.all_paths.len()));
        RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            vec![graph_array, start_array, end_array],
            &options,
        )
        .map_err(Into::into)
    }
}

impl RecordBatchStream for KleenePlusClosureStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for KleenePlusClosureStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.poll_inner(cx)
    }
}
