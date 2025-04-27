use axum::response::{IntoResponse, Response};
use graphfusion::QueryResults;

/// Wraps a [QueryResults] that can be converted into a [Response].
pub struct QueryResultsResponse(QueryResults);

impl From<QueryResults> for QueryResultsResponse {
    fn from(value: QueryResults) -> Self {
        QueryResultsResponse(value)
    }
}

impl IntoResponse for QueryResultsResponse {
    fn into_response(self) -> Response {
        todo!()
    }
}
