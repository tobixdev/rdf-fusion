use crate::AppState;
use crate::error::RdfFusionServerError;
use anyhow::anyhow;
use axum::body::Body;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use axum_extra::TypedHeader;
use headers::ContentType;
use rdf_fusion::error::LoaderError;
use rdf_fusion::io::{RdfFormat, RdfParser};

pub async fn handle_data_post(
    content_type: TypedHeader<ContentType>,
    State(state): State<AppState>,
    body: String,
) -> Result<Response, RdfFusionServerError> {
    let format =
        RdfFormat::from_media_type(&content_type.0.to_string()).ok_or_else(|| {
            RdfFusionServerError::BadRequest("Invalid content type.".to_owned())
        })?;
    let parser = RdfParser::from_format(format);

    // TODO logging
    state
        .store
        .load_from_reader(parser, body.as_bytes())
        .await
        .map_err(|error| match error {
            LoaderError::Parsing(err) => RdfFusionServerError::BadRequest(format!(
                "Error parsing {} RDF file: {}",
                format, err
            )),
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
