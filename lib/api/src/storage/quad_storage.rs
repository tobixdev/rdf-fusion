use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_planner::ExtensionPlanner;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::{
    GraphName, GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef,
    TriplePattern, Variable,
};
use std::fmt::Debug;
use std::sync::Arc;

#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait QuadStorage: Send + Sync {
    /// Returns the quad storage encoding.
    fn encoding(&self) -> QuadStorageEncoding;

    /// Returns a list of planners that support planning logical nodes requiring access to the
    /// storage layer.
    fn planners(&self) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>>;

    /// Loads the given quads into the storage.
    async fn extend(&self, quads: Vec<Quad>) -> Result<usize, StorageError>;

    /// Creates an empty named graph in the storage.
    async fn insert_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError>;

    /// Returns the list of named graphs in the storage.
    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError>;

    /// Returns whether `graph_name` is a named graph in the storage.
    async fn contains_named_graph<'a>(
        &self,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError>;

    /// Clears the entire storage.
    async fn clear(&self) -> Result<(), StorageError>;

    /// Clears the entire graph.
    async fn clear_graph<'a>(
        &self,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError>;

    /// Removes the entire named graph from the storage.
    async fn remove_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError>;

    /// Removes the given quad from the storage.
    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError>;

    /// Returns the number of quads in the storage.
    async fn len(&self) -> Result<usize, StorageError>;
}

/// The quad pattern evaluator is responsible for accessing the storage and returning a stream of
/// results that adhere to the given pattern.
///
/// # Consistency
///
/// A query plan most often contains multiple quad patterns that have access to the same storage.
/// It is the responsibility of the storage layer to ensure that the quad patterns use the same
/// snapshot of the storage layer.
#[async_trait]
pub trait QuadPatternEvaluator: Debug + Send + Sync {
    /// Returns the [QuadStorageEncoding] of the storage layer.
    fn storage_encoding(&self) -> QuadStorageEncoding;

    /// Returns a stream of quads that match the given pattern.
    ///
    /// The resulting stream must have a schema that projects to the variables provided in the
    /// arguments. Each emitted batch should have `batch_size` elements.
    fn evaluate_pattern(
        &self,
        graph: GraphName,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
        metrics: BaselineMetrics,
        batch_size: usize,
    ) -> DFResult<SendableRecordBatchStream>;
}
