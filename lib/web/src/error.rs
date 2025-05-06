use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

#[derive(thiserror::Error, Debug)]
pub enum GraphFusionServerError {
    #[error("Bad request: {0}")]
    BadRequest(String),
    #[error("Content Negotiation Failed: {0}")]
    ContentNegotiation(String),
    #[error("Server is read-only")]
    ReadOnly,
    #[error("Internal server error: {0}")]
    Internal(anyhow::Error),
}

impl IntoResponse for GraphFusionServerError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            GraphFusionServerError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            GraphFusionServerError::ContentNegotiation(msg) => (StatusCode::NOT_ACCEPTABLE, msg),
            GraphFusionServerError::ReadOnly => (
                StatusCode::FORBIDDEN,
                "Server is in read-only mode".to_owned(),
            ),
            GraphFusionServerError::Internal(e) => {
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            }
        };

        (status, message).into_response()
    }
}
