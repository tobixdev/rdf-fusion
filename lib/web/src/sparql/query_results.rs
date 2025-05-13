use axum::response::{IntoResponse, Response};
use rdf_fusion::QueryResults;

/// Wraps a [QueryResults] that can be converted into a [Response].
#[allow(unused)]
pub struct QueryResultsResponse(QueryResults);

impl From<QueryResults> for QueryResultsResponse {
    fn from(value: QueryResults) -> Self {
        QueryResultsResponse(value)
    }
}

impl IntoResponse for QueryResultsResponse {
    #[allow(clippy::todo, reason = "No production code yet")]
    fn into_response(self) -> Response {
        todo!()
    }
}
