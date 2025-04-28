use crate::results::QueryResults;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{Query, QueryExplanation, QueryOptions};
use crate::DFResult;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use model::{GraphNameRef, NamedNodeRef, Quad, QuadRef, SubjectRef, TermRef};

#[async_trait]
#[allow(clippy::len_without_is_empty)]
pub trait TripleStore {
    //
    // Querying
    //

    async fn contains(&self, quad: &QuadRef<'_>) -> Result<bool, DataFusionError>;
    async fn len(&self) -> DFResult<usize>;
    async fn quads_for_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<SendableRecordBatchStream>;

    async fn execute_query(
        &self,
        query: &Query,
        options: QueryOptions,
    ) -> Result<(QueryResults, Option<QueryExplanation>), QueryEvaluationError>;

    //
    // Loading
    //

    async fn load_quads(&self, quads: Vec<Quad>) -> Result<usize, DataFusionError>;

    //
    // Removing
    //

    async fn remove<'a>(&self, quad: QuadRef<'_>) -> Result<bool, DataFusionError>;
}
