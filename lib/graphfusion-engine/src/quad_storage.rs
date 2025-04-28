use crate::DFResult;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use model::{Quad, QuadRef};
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
    async fn load_quads(&self, quads: Vec<Quad>) -> DFResult<usize>;

    /// Removes the given quad from the storage.
    async fn remove<'a>(&self, quad: QuadRef<'_>) -> DFResult<bool>;
}
