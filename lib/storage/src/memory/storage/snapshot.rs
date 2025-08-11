use crate::memory::MemObjectIdMapping;
use crate::memory::storage::log::{LogChanges, MemLogSnapshot};
use crate::memory::storage::stream::{MemLogInsertionsStream, MemQuadPatternStream};
use datafusion::common::exec_err;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::physical_plan::coop::cooperative;
use datafusion::physical_plan::metrics::BaselineMetrics;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::patterns::compute_schema_for_triple_pattern;
use rdf_fusion_model::{NamedOrBlankNode, NamedOrBlankNodeRef, TriplePattern, Variable};
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
    pub fn evaluate_pattern(
        &self,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        triple_pattern: TriplePattern,
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
                &triple_pattern,
                blank_node_mode,
            )
            .inner(),
        );
        let Ok(log_insertion_stream) = self
            .changes
            .clone()
            .map(|c| {
                MemLogInsertionsStream::try_new(
                    &self.object_id_mapping,
                    self.encoding.clone(),
                    &active_graph,
                    graph_variable.as_ref(),
                    &triple_pattern,
                    blank_node_mode,
                    c.inserted.iter().cloned().collect(),
                    task_context.session_config().batch_size(),
                )
            })
            .transpose()
        else {
            // If we cannot find an object id, we know that the pattern does not match any quads.
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema.clone())));
        };

        Ok(Box::pin(cooperative(MemQuadPatternStream::new(
            schema,
            metrics,
            log_insertion_stream.map(Box::new),
        ))))
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
