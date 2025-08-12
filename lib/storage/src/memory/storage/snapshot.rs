use crate::memory::encoding::{
    EncodedActiveGraph, EncodedTermPattern, EncodedTriplePattern,
};
use crate::memory::object_id::DEFAULT_GRAPH_ID;
use crate::memory::storage::index::{IndexLookup, IndexSet, MemHashIndexIterator};
use crate::memory::storage::log::{LogChanges, MemLogSnapshot};
use crate::memory::storage::stream::{MemLogInsertionsStream, MemQuadPatternStream};
use crate::memory::storage::VersionNumber;
use crate::memory::MemObjectIdMapping;
use datafusion::common::exec_err;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
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

/// Provides a snapshot view on the storage. Other transactions can read and write to the storage
/// without changing the view of the snapshot.
#[derive(Debug, Clone)]
pub struct MemQuadStorageSnapshot {
    /// The encoding of the storage.
    encoding: QuadStorageEncoding,
    /// Object id mapping
    object_id_mapping: Arc<MemObjectIdMapping>,
    /// Changes in the log.
    changes: Option<Arc<LogChanges>>,
    /// Index set
    index_set: Arc<IndexSet>,
    /// The version number of the snapshot.
    version_number: VersionNumber,
}

impl MemQuadStorageSnapshot {
    /// Create a new [MemQuadStorageSnapshot].
    ///
    /// This function computes the changes in the `log_snapshot` and is thus not trivial. However,
    /// caching the changes computation can be beneficial in scenarios where the log is accessed
    /// multiple times (e.g., multiple quad patterns in the same query).
    pub async fn new_with_computed_changes(
        encoding: QuadStorageEncoding,
        object_id_mapping: Arc<MemObjectIdMapping>,
        log_snapshot: MemLogSnapshot,
    ) -> Self {
        let changes = log_snapshot.compute_changes().await;
        Self {
            encoding,
            object_id_mapping,
            changes: changes.map(Arc::new),
        }
    }

    /// Returns the encoding of the underlying storage.
    pub fn storage_encoding(&self) -> &QuadStorageEncoding {
        &self.encoding
    }

    /// Returns a stream that evaluates the given pattern.
    #[allow(clippy::too_many_arguments)]
    pub async fn evaluate_pattern(
        &self,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        metrics: BaselineMetrics,
        task_context: Arc<TaskContext>,
        partition: usize,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("Only partition 0 is supported for now.");
        }

        let schema = Arc::clone(
            compute_schema_for_triple_pattern(
                self.storage_encoding(),
                graph_variable.as_ref().map(|v| v.as_ref()),
                &pattern,
                blank_node_mode,
            )
            .inner(),
        );
        let changes = self.changes.clone();

        let Ok(active_graph) = (self.encode_active_graph(active_graph)) else {
            // The error is only triggered if no graph can be decoded.
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema.clone())));
        };

        let Ok(pattern) = (self.encode_triple_pattern(&pattern, blank_node_mode)) else {
            // For the pattern, a single unknown term causes the result to be empty.
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema.clone())));
        };

        let index_scans = self
            .create_index_scans(
                &active_graph,
                graph_variable.as_ref(),
                &pattern,
                changes.as_deref(),
            )
            .await?;

        let log_insertion_stream = changes.map(|c| {
            Box::new(MemLogInsertionsStream::new(
                schema.clone(),
                self.storage_encoding().clone(),
                &active_graph,
                graph_variable.as_ref(),
                &pattern,
                blank_node_mode,
                c.inserted.iter().cloned().collect(),
                task_context.session_config().batch_size(),
            ))
        });

        Ok(Box::pin(cooperative(MemQuadPatternStream::new(
            schema,
            metrics,
            index_scans,
            log_insertion_stream,
        ))))
    }

    async fn create_index_scans(
        &self,
        active_graph: &EncodedActiveGraph,
        graph_name_variable: Option<&Variable>,
        pattern: &EncodedTriplePattern,
        changes: Option<&LogChanges>,
    ) -> DFResult<Vec<MemHashIndexIterator>> {
        if changes.is_some() {
            return exec_err!(
                "Index scans with pending log changes are not supported yet."
            );
        }

        let scan_pattern = |graph| {
            IndexLookup([
                graph,
                pattern.subject.clone(),
                pattern.predicate.clone(),
                pattern.object.clone(),
            ])
        };

        Ok(match active_graph {
            EncodedActiveGraph::DefaultGraph => {
                vec![
                    self.index_set
                        .scan(
                            scan_pattern(EncodedTermPattern::ObjectId(
                                DEFAULT_GRAPH_ID.0,
                            )),
                            self.version_number,
                        )
                        .await?,
                ]
            }
            EncodedActiveGraph::AllGraphs => {
                vec![
                    self.index_set
                        .scan(
                            scan_pattern(EncodedTermPattern::Wildcard),
                            self.version_number,
                        )
                        .await?,
                ]
            }
            EncodedActiveGraph::Union(graphs) => graphs
                .iter()
                .map(|g| {
                    self.index_set.scan(
                        scan_pattern(EncodedTermPattern::ObjectId(g.0)),
                        self.version_number,
                    )
                })
                .collect::<DFResult<Vec<_>>>()?,
            EncodedActiveGraph::AnyNamedGraph => {
                vec![
                    self.index_set
                        .scan(
                            scan_pattern(EncodedTermPattern::Wildcard),
                            self.version_number,
                        )
                        .await?,
                ]
            }
        })
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
    pub async fn len(&self) -> usize {
        let Some(changes) = self.changes.as_ref() else {
            return 0;
        };

        changes.inserted.len()
    }

    /// Returns the number of quads in the storage.
    pub async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        let Some(changes) = self.changes.as_ref() else {
            return Ok(vec![]);
        };

        Iterator::chain(
            changes.created_named_graphs.iter().copied(),
            changes
                .inserted
                .iter()
                .filter(|q| !q.graph_name.is_default_graph())
                .map(|q| q.graph_name.as_encoded_object_id()),
        )
        .map(|g| {
            self.object_id_mapping
                .decode_named_graph(g)
                .map_err(StorageError::from)
        })
        .collect()
    }

    /// Returns whether the storage contains the named graph `graph_name`.
    pub async fn contains_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> bool {
        let Some(object_id) = self
            .object_id_mapping
            .try_get_encoded_object_id_from_term(graph_name)
        else {
            // Object IDs are not garbage collected. If the object ID is not found, the name has
            // never been stored in the system.
            return false;
        };

        let Some(changes) = self.changes.as_ref() else {
            return false;
        };

        if changes.created_named_graphs.contains(&object_id) {
            return true;
        }

        changes.inserted.iter().any(|q| {
            !q.graph_name.is_default_graph()
                && q.graph_name.as_encoded_object_id() == object_id
        })
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
            BlankNodeMatchingMode::Variable => EncodedTermPattern::Variable,
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
        TermPattern::Variable(_) => EncodedTermPattern::Variable,
    })
}
