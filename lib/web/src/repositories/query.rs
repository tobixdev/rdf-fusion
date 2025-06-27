use crate::error::RdfFusionServerError;
use crate::AppState;
use anyhow::anyhow;
use axum::extract::State;
use axum::response::{IntoResponse, Response};
use rdf_fusion::io::RdfFormat;
use rdf_fusion::model::{GraphName, IriParseError, NamedNode, NamedOrBlankNode};
use rdf_fusion::results::QueryResultsFormat;
use rdf_fusion::store::Store;
use crate::repositories::query_results::QueryResultsResponse;
use crate::repositories::service_description::{generate_service_description, EndpointKind, ServiceDescription};
use crate::repositories::sparql_query_params::SparqlQueryParams;

pub async fn handle_query_get(
    State(state): State<AppState>,
    query_params: SparqlQueryParams,
    rdf_format: RdfFormat,
    query_format: QueryResultsFormat,
) -> Result<HandleQueryResponse, RdfFusionServerError> {

    let Some(query) = &query_params.query else {
        return Ok(generate_service_description(
            rdf_format,
            EndpointKind::Query,
            query_params.default_graph_as_union,
        )
        .into());
    };

    if query.is_empty() {
        return Ok(generate_service_description(
            rdf_format,
            EndpointKind::Query,
            query_params.default_graph_as_union,
        )
        .into());
    }

    Ok(evaluate_sparql_query(&state.store, &query_params, query, rdf_format, query_format)
        .await?
        .into())
}

async fn evaluate_sparql_query(
    store: &Store,
    params: &SparqlQueryParams,
    query: &str,
    rdf_format: RdfFormat,
    query_format: QueryResultsFormat,
) -> Result<QueryResultsResponse, RdfFusionServerError> {
    let mut query = rdf_fusion::Query::parse(query, Some(params.base_uri.as_str()))
        .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?;

    if params.default_graph_as_union {
        query.dataset_mut().set_default_graph_as_union()
    } else if !params.default_graph_uris.is_empty() || !params.named_graph_uris.is_empty() {
        query.dataset_mut().set_default_graph(
            params
                .default_graph_uris
                .iter()
                .map(|e| Ok(NamedNode::new(e)?.into()))
                .collect::<Result<Vec<GraphName>, IriParseError>>()
                .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?,
        );
        query.dataset_mut().set_available_named_graphs(
            params
                .named_graph_uris
                .iter()
                .map(|e| Ok(NamedNode::new(e)?.into()))
                .collect::<Result<Vec<NamedOrBlankNode>, IriParseError>>()
                .map_err(|e| RdfFusionServerError::BadRequest(e.to_string()))?,
        );
    }

    store
        .query_opt(query, params.to_query_options())
        .await
        .map(|q| QueryResultsResponse::new(q, rdf_format, query_format))
        .map_err(|e| RdfFusionServerError::Internal(anyhow!(e)))
}

/// Holds any of the possible responses from a query request.
pub enum HandleQueryResponse {
    ServiceDescription(ServiceDescription),
    QueryResults(QueryResultsResponse),
}

impl IntoResponse for HandleQueryResponse {
    fn into_response(self) -> Response {
        match self {
            HandleQueryResponse::ServiceDescription(sd) => sd.into_response(),
            HandleQueryResponse::QueryResults(qr) => qr.into_response(),
        }
    }
}

impl From<ServiceDescription> for HandleQueryResponse {
    fn from(value: ServiceDescription) -> Self {
        Self::ServiceDescription(value)
    }
}

impl From<QueryResultsResponse> for HandleQueryResponse {
    fn from(value: QueryResultsResponse) -> Self {
        Self::QueryResults(value)
    }
}
