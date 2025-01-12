use crate::engine::DFResult;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use oxrdf::{GraphNameRef, NamedNodeRef, Quad, QuadRef, SubjectRef, TermRef};

#[async_trait]
pub trait TripleStore {
    async fn contains(&self, quad: &QuadRef<'_>) -> Result<bool, DataFusionError>;
    async fn len(&self) -> Result<usize, DataFusionError>;
    async fn load_quads(&self, quads: Vec<Quad>) -> Result<usize, DataFusionError>;
    async fn quads_for_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<SendableRecordBatchStream>;
}
