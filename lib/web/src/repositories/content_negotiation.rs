use crate::error::RdfFusionServerError;
use crate::AppState;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use headers::HeaderMapExt;
use headers_accept::Accept;
use mediatype::names::{
    APPLICATION, CSV, JSON, N3, N_QUADS, N_TRIPLES, PLAIN, TEXT, TRIG, TURTLE, XML,
};
use mediatype::{MediaType, Name};
use rdf_fusion::io::RdfFormat;
use rdf_fusion::results::QueryResultsFormat;

/// Handles the content-negotiation for requests that return RDF data.
impl FromRequestParts<AppState> for RdfFormat {
    type Rejection = RdfFusionServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        static MEDIA_TYPES: [MediaType<'_>; 8] = [
            MediaType::new(TEXT, PLAIN),
            MediaType::new(APPLICATION, N_QUADS),
            MediaType::new(APPLICATION, N_TRIPLES),
            MediaType::new(APPLICATION, TRIG),
            MediaType::new(APPLICATION, TURTLE),
            MediaType::new(APPLICATION, N3),
            MediaType::new(APPLICATION, XML),
            MediaType::new(APPLICATION, Name::new_unchecked("rdf+xml")),
        ];
        static DEFAULT_MEDIA_TYPE: MediaType<'_> = MediaType::new(APPLICATION, N_QUADS);

        let accept = parts.headers.typed_get::<Accept>();
        let media_type = content_negotiation(
            accept,
            &MEDIA_TYPES,
            &DEFAULT_MEDIA_TYPE,
            "application/sparql-results+json or text/tsv",
        )?;

        RdfFormat::from_media_type(media_type.to_string().as_str()).ok_or(
            RdfFusionServerError::BadRequest(format!(
                "Could not convert negotiated media type '{media_type}' to internal representation."
            )),
        )
    }
}

/// Handles the content-negotiation for requests that return query results.
impl FromRequestParts<AppState> for QueryResultsFormat {
    type Rejection = RdfFusionServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        static MEDIA_TYPES: [MediaType<'_>; 8] = [
            MediaType::new(TEXT, PLAIN),
            MediaType::new(TEXT, CSV),
            MediaType::new(TEXT, Name::new_unchecked("tsv")),
            MediaType::new(APPLICATION, JSON),
            MediaType::new(APPLICATION, Name::new_unchecked("sparql-results+json")),
            MediaType::new(APPLICATION, Name::new_unchecked("sparql-results+xml")),
            MediaType::new(APPLICATION, Name::new_unchecked("tab-separated-values")),
            MediaType::new(APPLICATION, XML),
        ];
        static DEFAULT_MEDIA_TYPE: MediaType<'_> = MediaType::new(APPLICATION, JSON);

        let accept = parts.headers.typed_get::<Accept>();
        let media_type = content_negotiation(
            accept,
            &MEDIA_TYPES,
            &DEFAULT_MEDIA_TYPE,
            "application/sparql-results+json or text/tsv",
        )?;

        QueryResultsFormat::from_media_type(media_type.to_string().as_str()).ok_or(
            RdfFusionServerError::BadRequest(format!(
                "Could not convert negotiated media type '{media_type}' to internal representation."
            )),
        )
    }
}

fn content_negotiation<'media>(
    accept: Option<Accept>,
    available: &'media [MediaType<'media>],
    default: &'media MediaType<'media>,
    example: &str,
) -> Result<MediaType<'media>, RdfFusionServerError> {
    let Some(accept) = accept else {
        return Ok(default.clone());
    };

    match accept.negotiate(available) {
        None => Err(RdfFusionServerError::ContentNegotiation(format!(
            "The accept header does not provide any accepted format like {example}."
        ))),
        Some(result) => Ok(result.clone()),
    }
}
