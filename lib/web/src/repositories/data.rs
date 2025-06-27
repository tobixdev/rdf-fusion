use crate::error::RdfFusionServerError;
use crate::AppState;
use anyhow::anyhow;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use rdf_fusion::error::LoaderError;
use rdf_fusion::io::{RdfFormat, RdfParser};

pub async fn handle_data_post(
    State(state): State<AppState>,
    format: RdfFormat,
    body: String,
) -> Result<Response, RdfFusionServerError> {
    let parser = RdfParser::from_format(format);

    // TODO logging
    state
        .store
        .load_from_reader(parser, body.as_bytes())
        .await
        .map_err(|error| match error {
            LoaderError::Parsing(err) => RdfFusionServerError::BadRequest(err.to_string()),
            LoaderError::Storage(_) => {
                RdfFusionServerError::Internal(anyhow!("Error with storage layer."))
            }
            LoaderError::InvalidBaseIri { .. } => {
                RdfFusionServerError::BadRequest("Invalid base IRI.".to_owned())
            }
        })?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}
