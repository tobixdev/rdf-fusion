use crate::DFResult;
use arrow_rdf::as_enc_term_array;
use arrow_rdf::encoded::{EncRdfTermBuilder, FromEncodedTerm};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{exec_datafusion_err, internal_err, plan_err, SchemaExt};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use datamodel::{GraphName, GraphNameRef, Term, TermRef};
use futures::{Stream, StreamExt};
use graphfusion_logical::paths::PATH_TABLE_SCHEMA;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

// Represents a path in the closure.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct Path {
    graph: GraphName,
    start: Term,
    end: Term,
}

#[derive(Debug)]
pub struct KleenePlusPathExec {
    plan_properties: PlanProperties,
    inner: Arc<dyn ExecutionPlan>,
}

impl KleenePlusPathExec {
    /// Creates a new [KleenePlusPathExec].
    pub fn try_new(inner: Arc<dyn ExecutionPlan>) -> DFResult<Self> {
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
            EquivalenceProperties::new(inner.schema().clone()),
            Partitioning::UnknownPartitioning(1), // Computation requires all data in one partition
            EmissionType::Final,                  // Emits results only after full computation
            Boundedness::Bounded,                 // Assumes the closure computation terminates
        );
        Ok(Self {
            plan_properties,
            inner,
        })
    }
}

impl ExecutionPlan for KleenePlusPathExec {
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
        // Recreate the operator using try_new to ensure invariants hold
        let exec = KleenePlusPathExec::try_new(children[0].clone())?;
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

        let input_stream = self.inner.execute(0, context)?;
        let schema = self.schema().clone();

        Ok(Box::pin(KleenePlusPathStream::new(input_stream, schema)))
    }
}

impl DisplayAs for KleenePlusPathExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "KleenePlusPathExec")
            }
        }
    }
}

// Helper struct to hold the state of our stream
struct KleenePlusPathStream {
    state: KleenePlusPathStreamState,
    schema: SchemaRef,
}

// Enum to track the state of our stream processing
enum KleenePlusPathStreamState {
    // Initial state - need to collect input batches
    CollectingInput {
        stream: SendableRecordBatchStream,
        batches: Vec<RecordBatch>,
    },
    // Computing the closure
    Computing {
        initial_paths_map: HashMap<GraphName, HashSet<(Term, Term)>>,
        all_paths: HashSet<Path>,
        current_delta: Vec<Path>,
    },
    // Done - either yielding final batch or finished
    Done,
    // Error state
    Error,
}

impl KleenePlusPathStream {
    fn new(input: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        Self {
            state: KleenePlusPathStreamState::CollectingInput {
                stream: input,
                batches: Vec::new(),
            },
            schema,
        }
    }
}

impl RecordBatchStream for KleenePlusPathStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for KleenePlusPathStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                KleenePlusPathStreamState::CollectingInput { stream, batches } => {
                    match stream.poll_next_unpin(cx) {
                        Poll::Ready(Some(Ok(batch))) => {
                            batches.push(batch);
                            continue;
                        }
                        Poll::Ready(None) => {
                            // All input collected, move to computing state
                            let collected_batches = std::mem::take(batches);
                            if collected_batches.is_empty() {
                                this.state = KleenePlusPathStreamState::Done;
                                return Poll::Ready(Some(Ok(RecordBatch::new_empty(
                                    this.schema.clone(),
                                ))));
                            }

                            // Initialize computation state
                            let mut initial_paths_map: HashMap<GraphName, HashSet<(Term, Term)>> =
                                HashMap::new();
                            let mut all_paths: HashSet<Path> = HashSet::new();
                            let mut current_delta: Vec<Path> = Vec::new();

                            // Process initial batches
                            for batch in collected_batches {
                                if let Err(e) = Self::process_batch(
                                    &batch,
                                    &mut initial_paths_map,
                                    &mut all_paths,
                                    &mut current_delta,
                                ) {
                                    this.state = KleenePlusPathStreamState::Error;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }

                            this.state = KleenePlusPathStreamState::Computing {
                                initial_paths_map,
                                all_paths,
                                current_delta,
                            };
                            continue;
                        }
                        Poll::Ready(Some(Err(e))) => {
                            this.state = KleenePlusPathStreamState::Error;
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                KleenePlusPathStreamState::Computing {
                    initial_paths_map,
                    ref mut all_paths,
                    current_delta,
                } => {
                    // Compute one iteration of the closure
                    let mut next_delta = Vec::new();

                    for path_ab in current_delta.iter() {
                        if let Some(initial_paths_from_b) = initial_paths_map.get(&path_ab.graph) {
                            for (initial_start_b, initial_end_c) in initial_paths_from_b {
                                if &path_ab.end == initial_start_b {
                                    let path_ac = Path {
                                        graph: path_ab.graph.clone(),
                                        start: path_ab.start.clone(),
                                        end: initial_end_c.clone(),
                                    };

                                    if all_paths.insert(path_ac.clone()) {
                                        next_delta.push(path_ac);
                                    }
                                }
                            }
                        }
                    }

                    if next_delta.is_empty() {
                        // Closure computation complete, create final batch
                        return match Self::create_output_batch(all_paths, &this.schema) {
                            Ok(batch) => {
                                this.state = KleenePlusPathStreamState::Done;
                                Poll::Ready(Some(Ok(batch)))
                            }
                            Err(e) => {
                                this.state = KleenePlusPathStreamState::Error;
                                Poll::Ready(Some(Err(e)))
                            }
                        };
                    }

                    *current_delta = next_delta;
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
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
}

impl KleenePlusPathStream {
    fn process_batch(
        batch: &RecordBatch,
        initial_paths_map: &mut HashMap<GraphName, HashSet<(Term, Term)>>,
        all_paths: &mut HashSet<Path>,
        current_delta: &mut Vec<Path>,
    ) -> DFResult<()> {
        let graph_names = as_enc_term_array(batch.column(0))?;
        let starts = as_enc_term_array(batch.column(1))?;
        let ends = as_enc_term_array(batch.column(2))?;

        for i in 0..batch.num_rows() {
            let graph = GraphNameRef::from_enc_array(graph_names, i).map_err(|_| {
                exec_datafusion_err!("Could not obtain graph value from inner paths.")
            })?;
            let start = TermRef::from_enc_array(starts, i).map_err(|_| {
                exec_datafusion_err!("Could not obtain start value from inner paths.")
            })?;
            let end = TermRef::from_enc_array(ends, i).map_err(|_| {
                exec_datafusion_err!("Could not obtain end value from inner paths.")
            })?;

            let path = Path {
                graph: graph.into_owned(),
                start: start.to_owned(),
                end: end.to_owned(),
            };

            let path_tuple = (start.to_owned(), end.to_owned());
            initial_paths_map
                .entry(graph.into_owned())
                .or_default()
                .insert(path_tuple);

            if all_paths.insert(path.clone()) {
                current_delta.push(path);
            }
        }
        Ok(())
    }

    fn create_output_batch(all_paths: &HashSet<Path>, schema: &SchemaRef) -> DFResult<RecordBatch> {
        let mut graph_builder = EncRdfTermBuilder::new();
        let mut start_builder = EncRdfTermBuilder::new();
        let mut end_builder = EncRdfTermBuilder::new();

        for path in all_paths {
            match &path.graph {
                GraphName::NamedNode(named) => graph_builder.append_named_node(named.as_str())?,
                GraphName::BlankNode(bnode) => graph_builder.append_blank_node(bnode.as_str())?,
                GraphName::DefaultGraph => graph_builder.append_null()?,
            }
            start_builder.append_decoded_term(&path.start.as_ref().into_decoded())?;
            end_builder.append_decoded_term(&path.end.as_ref().into_decoded())?;
        }

        let graph_array = graph_builder.finish()?;
        let start_array = start_builder.finish()?;
        let end_array = end_builder.finish()?;

        RecordBatch::try_new(schema.clone(), vec![graph_array, start_array, end_array])
            .map_err(Into::into)
    }
}
