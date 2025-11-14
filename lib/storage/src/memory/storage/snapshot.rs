use crate::index::IndexPermutations;
use crate::memory::MemObjectIdMapping;
use crate::memory::encoding::{
    EncodedActiveGraph, EncodedTermPattern, EncodedTriplePattern,
};
use crate::memory::storage::quad_index::MemQuadIndex;
use crate::memory::storage::scan::PlannedPatternScan;
use crate::memory::storage::scan_instructions::{
    MemIndexScanInstruction, MemIndexScanInstructions,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::object_id::UnknownObjectIdError;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{BlankNodeMatchingMode, DFResult, NamedNodePattern};
use rdf_fusion_model::{
    NamedOrBlankNode, NamedOrBlankNodeRef, TermPattern, TriplePattern, Variable,
};
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

/// Provides a snapshot view on the storage. Other transactions can read and write to the storage
/// without changing the view of the snapshot.
#[derive(Debug, Clone)]
pub struct MemQuadStorageSnapshot {
    /// The encoding of the storage.
    encoding: QuadStorageEncoding,
    /// Object id mapping
    object_id_mapping: Arc<MemObjectIdMapping>,
    /// Holds a read lock on the index set. Holding the lock prevents concurrent modifications to
    /// the index.
    index_permutations: Arc<OwnedRwLockReadGuard<IndexPermutations<MemQuadIndex>>>,
}

/// The result of a [MemQuadStorageSnapshot::plan_pattern_evaluation].
#[derive(Debug, Clone)]
pub enum PlanPatternScanResult {
    /// The result is guaranteed to be empty.
    Empty(SchemaRef),
    /// The planned scan for the pattern.
    PatternScan(PlannedPatternScan),
}

impl PlanPatternScanResult {
    /// Returns a stream that returns the result of the planned pattern scan.
    pub fn create_stream(&self, metrics: BaselineMetrics) -> SendableRecordBatchStream {
        match self {
            PlanPatternScanResult::Empty(schema) => {
                Box::pin(EmptyRecordBatchStream::new(Arc::clone(schema)))
            }
            PlanPatternScanResult::PatternScan(scan) => {
                scan.clone().create_stream(metrics)
            }
        }
    }
}

impl MemQuadStorageSnapshot {
    /// Create a new [MemQuadStorageSnapshot].
    pub fn new(
        encoding: QuadStorageEncoding,
        object_id_mapping: Arc<MemObjectIdMapping>,
        index_set: Arc<OwnedRwLockReadGuard<IndexPermutations<MemQuadIndex>>>,
    ) -> Self {
        Self {
            encoding,
            object_id_mapping,
            index_permutations: index_set,
        }
    }

    /// Returns the encoding of the underlying storage.
    pub fn storage_encoding(&self) -> &QuadStorageEncoding {
        &self.encoding
    }

    /// Returns a stream that evaluates the given pattern.
    #[allow(clippy::too_many_arguments)]
    pub async fn plan_pattern_evaluation(
        &self,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> DFResult<PlanPatternScanResult> {
        let schema = Arc::clone(
            compute_schema_for_triple_pattern(
                self.storage_encoding(),
                graph_variable.as_ref().map(|v| v.as_ref()),
                &pattern,
                blank_node_mode,
            )
            .inner(),
        );

        let Ok(enc_active_graph) = self.encode_active_graph(&active_graph) else {
            // The error is only triggered if no graph can be decoded.
            return Ok(PlanPatternScanResult::Empty(schema));
        };

        let Ok(enc_pattern) = self.encode_triple_pattern(&pattern, blank_node_mode)
        else {
            // For the pattern, a single unknown term causes the result to be empty.
            return Ok(PlanPatternScanResult::Empty(schema));
        };

        let scan_instructions = MemIndexScanInstructions::new_gspo([
            MemIndexScanInstruction::from_active_graph(
                &enc_active_graph,
                graph_variable.as_ref(),
            ),
            MemIndexScanInstruction::from(enc_pattern.subject.clone()),
            MemIndexScanInstruction::from(enc_pattern.predicate.clone()),
            MemIndexScanInstruction::from(enc_pattern.object.clone()),
        ]);

        let index = self.index_permutations.choose_index(&scan_instructions);
        Ok(PlanPatternScanResult::PatternScan(PlannedPatternScan::new(
            schema,
            Arc::clone(&self.index_permutations),
            index,
            Box::new(scan_instructions),
            graph_variable,
            Box::new(pattern),
        )))
    }

    /// Returns a [PlanPatternScanResult] that extracts all quads from the storage from an arbitrary
    /// index.
    pub async fn stream_quads(&self) -> DFResult<PlanPatternScanResult> {
        self.plan_pattern_evaluation(
            ActiveGraph::AllGraphs,
            Some(Variable::new_unchecked(COL_GRAPH)),
            TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
                predicate: NamedNodePattern::Variable(Variable::new_unchecked(
                    COL_PREDICATE,
                )),
                object: TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
            },
            BlankNodeMatchingMode::Filter,
        )
        .await
    }

    /// Encodes the triple pattern.
    ///
    /// If this operation fails, the result of a triple lookup will always be empty.
    fn encode_triple_pattern(
        &self,
        pattern: &TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> Result<EncodedTriplePattern, UnknownObjectIdError> {
        let subject = encode_term_pattern(
            self.object_id_mapping.as_ref(),
            &pattern.subject,
            blank_node_mode,
        )?;
        let predicate = encode_term_pattern(
            self.object_id_mapping.as_ref(),
            &pattern.predicate.clone().into(),
            blank_node_mode,
        )?;
        let object = encode_term_pattern(
            self.object_id_mapping.as_ref(),
            &pattern.object,
            blank_node_mode,
        )?;
        Ok(EncodedTriplePattern {
            subject,
            predicate,
            object,
        })
    }

    /// Encodes the active graph.
    ///
    /// This method only returns an error if *no* active graph can be decoded. Otherwise, the subset
    /// of graphs that could be decoded is returned. The error indicates that no triple in the store
    /// can match the pattern (because no graph of the [ActiveGraph::Union] is known).
    fn encode_active_graph(
        &self,
        active_graph: &ActiveGraph,
    ) -> Result<EncodedActiveGraph, UnknownObjectIdError> {
        Ok(match active_graph {
            ActiveGraph::DefaultGraph => EncodedActiveGraph::DefaultGraph,
            ActiveGraph::AllGraphs => EncodedActiveGraph::AllGraphs,
            ActiveGraph::Union(graphs) => {
                let decoded = graphs
                    .iter()
                    .filter_map(|g| {
                        self.object_id_mapping
                            .try_get_encoded_object_id_from_graph_name(g.as_ref())
                    })
                    .collect::<Vec<_>>();

                if decoded.is_empty() {
                    return Err(UnknownObjectIdError);
                }

                EncodedActiveGraph::Union(decoded)
            }
            ActiveGraph::AnyNamedGraph => EncodedActiveGraph::AnyNamedGraph,
        })
    }

    /// Returns the number of quads in the storage.
    pub fn len(&self) -> usize {
        self.index_permutations.as_ref().len()
    }

    /// Returns the number of quads in the storage.
    pub fn named_graphs(&self) -> Vec<NamedOrBlankNode> {
        self.index_permutations
            .as_ref()
            .named_graphs()
            .into_iter()
            .map(|g| self.object_id_mapping.decode_named_graph(g))
            .collect::<Result<Vec<_>, _>>()
            .expect("Index only contains valid named graphs")
    }

    /// Returns whether the storage contains the named graph `graph_name`.
    pub fn contains_named_graph(&self, graph_name: NamedOrBlankNodeRef<'_>) -> bool {
        let Some(object_id) = self
            .object_id_mapping
            .try_get_encoded_object_id_from_term(graph_name)
        else {
            return false;
        };

        self.index_permutations
            .as_ref()
            .contains_named_graph(object_id)
    }
}

fn encode_term_pattern(
    object_id_mapping: &MemObjectIdMapping,
    pattern: &TermPattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> Result<EncodedTermPattern, UnknownObjectIdError> {
    Ok(match pattern {
        TermPattern::NamedNode(nn) => {
            let object_id = object_id_mapping
                .try_get_encoded_object_id_from_term(nn.as_ref())
                .ok_or(UnknownObjectIdError)?;
            EncodedTermPattern::ObjectId(object_id)
        }
        TermPattern::BlankNode(bnode) => match blank_node_mode {
            BlankNodeMatchingMode::Variable => {
                EncodedTermPattern::Variable(bnode.as_str().to_owned())
            }
            BlankNodeMatchingMode::Filter => {
                let object_id = object_id_mapping
                    .try_get_encoded_object_id_from_term(bnode.as_ref())
                    .ok_or(UnknownObjectIdError)?;
                EncodedTermPattern::ObjectId(object_id)
            }
        },
        TermPattern::Literal(lit) => {
            let object_id = object_id_mapping
                .try_get_encoded_object_id_from_term(lit.as_ref())
                .ok_or(UnknownObjectIdError)?;
            EncodedTermPattern::ObjectId(object_id)
        }
        TermPattern::Variable(var) => {
            EncodedTermPattern::Variable(var.as_str().to_owned())
        }
    })
}
