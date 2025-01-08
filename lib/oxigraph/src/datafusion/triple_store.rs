use async_trait::async_trait;
use datafusion::common::DataFusionError;
use oxrdf::{Quad, QuadRef};

#[async_trait]
pub trait TripleStore {
    async fn load_from_reader(&self, quads: Vec<Quad>) -> Result<(), DataFusionError>;
    async fn contains(&self, quad: &QuadRef<'_>) -> Result<bool, DataFusionError>;
}
