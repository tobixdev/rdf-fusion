use crate::memory::encoding::{
    EncodedActiveGraph, EncodedTermPattern, EncodedTriplePattern,
};
use crate::memory::storage::index::IndexSet;
use crate::memory::storage::stream::MemQuadPatternStream;
use crate::memory::storage::VersionNumber;
use crate::memory::MemObjectIdMapping;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::EmptyRecordBatchStream;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::object_id::UnknownObjectIdError;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_logical::ActiveGraph;
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
    /// Index set
    index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
    /// The version number of the snapshot.
    version_number: VersionNumber,
}

impl MemQuadStorageSnapshot {
    /// Create a new [MemQuadStorageSnapshot].
    pub fn new(
        encoding: QuadStorageEncoding,
        object_id_mapping: Arc<MemObjectIdMapping>,
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
    ) -> Self {
        Self {
            encoding,
            object_id_mapping,
            version_number: index_set.version_number(),
            index_set,
        }
    }

    /// Returns the encoding of the underlying storage.
    pub fn storage_encoding(&self) -> &QuadStorageEncoding {
        &self.encoding
    }

    /// Returns a stream that evaluates the given pattern.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_pattern(
        &self,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        metrics: BaselineMetrics,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = Arc::clone(
            compute_schema_for_triple_pattern(
                self.storage_encoding(),
                graph_variable.as_ref().map(|v| v.as_ref()),
                &pattern,
                blank_node_mode,
            )
            .inner(),
        );

        let Ok(active_graph) = self.encode_active_graph(active_graph) else {
            // The error is only triggered if no graph can be decoded.
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema.clone())));
        };

        let Ok(pattern) = self.encode_triple_pattern(&pattern, blank_node_mode) else {
            // For the pattern, a single unknown term causes the result to be empty.
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema.clone())));
        };

        Ok(Box::pin(cooperative(MemQuadPatternStream::new(
            schema,
            self.index_set.clone(),
            self.version_number,
            active_graph,
            graph_variable,
            pattern,
            metrics,
        ))))
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
        active_graph: ActiveGraph,
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

                if decoded.len() == 0 {
                    return Err(UnknownObjectIdError);
                }

                EncodedActiveGraph::Union(decoded)
            }
            ActiveGraph::AnyNamedGraph => EncodedActiveGraph::AnyNamedGraph,
        })
    }

    /// Returns the number of quads in the storage.
    pub async fn len(&self) -> Result<usize, StorageError> {
        self.index_set.as_ref().len().await
    }

    /// Returns the number of quads in the storage.
    pub async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        self.index_set.as_ref().named_graphs().await
    }

    /// Returns whether the storage contains the named graph `graph_name`.
    pub async fn contains_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        self.index_set
            .as_ref()
            .contains_named_graph(graph_name)
            .await
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
