use crate::error::StorageError;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use model::{GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef};
use std::sync::Arc;

#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait QuadStorage: Send + Sync {
    /// Returns the table name of this [QuadStorage]. This name is used to register a table in the
    /// DataFusion engine.
    fn table_name(&self) -> &str;

    /// Returns the [TableProvider] for this [QuadStorage]. This provider is registered in the
    /// DataFusion session and used for planning the execution of queries.
    fn table_provider(&self) -> Arc<dyn TableProvider>;

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
    async fn clear_graph<'a>(&self, graph_name: GraphNameRef<'a>) -> Result<(), StorageError>;

    /// Removes the entire named graph from the storage.
    async fn remove_named_graph(
        &self,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError>;

    /// Removes the given quad from the storage.
    async fn remove(&self, quad: QuadRef<'_>) -> Result<bool, StorageError>;
}
