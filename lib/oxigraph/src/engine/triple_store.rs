use async_trait::async_trait;
use datafusion::common::DataFusionError;
use oxrdf::{Quad, QuadRef};

#[async_trait]
pub trait TripleStore {
    async fn contains(&self, quad: &QuadRef<'_>) -> Result<bool, DataFusionError>;
    async fn len(&self) -> Result<usize, DataFusionError>;
    async fn load_quads(&self, quads: Vec<Quad>) -> Result<usize, DataFusionError>;
}
