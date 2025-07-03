use crate::error::RdfFusionServerError;
use crate::repositories::service_description::{
    generate_service_description, EndpointKind, ServiceDescription,
};
use crate::repositories::sparql_query_params::SparqlQueryParams;
use crate::AppState;
use anyhow::{anyhow, Context};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use rdf_fusion::io::{RdfFormat, RdfSerializer};
use rdf_fusion::model::{GraphName, IriParseError, NamedNode, NamedOrBlankNode};
use rdf_fusion::results::{QueryResultsFormat, QueryResultsSerializer};
use rdf_fusion::store::Store;
use rdf_fusion::QueryResults;

pub async fn handle_query_get(
    State(state): State<AppState>,
    query_params: SparqlQueryParams,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> Result<HandleQueryResponse, RdfFusionServerError> {
    let Some(query) = &query_params.query else {
        return Ok(generate_service_description(
            rdf_format?,
            EndpointKind::Query,
            query_params.default_graph_as_union,
        )
        .into());
    };

    if query.is_empty() {
        return Ok(generate_service_description(
            rdf_format?,
            EndpointKind::Query,
            query_params.default_graph_as_union,
        )
        .into());
    }

    evaluate_sparql_query(&state.store, &query_params, query, rdf_format, query_format).await
}

async fn evaluate_sparql_query(
    store: &Store,
    params: &SparqlQueryParams,
    query: &str,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> Result<HandleQueryResponse, RdfFusionServerError> {
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

    let query_result = store
        .query_opt(query, params.to_query_options())
        .await
        .map_err(|e| RdfFusionServerError::Internal(anyhow!(e)))?;
    serialize_query_result(query_result, rdf_format, query_format)
        .await
        .map_err(|e| RdfFusionServerError::Internal(anyhow!(e)))
}

async fn serialize_query_result(
    query_result: QueryResults,
    rdf_format: Result<RdfFormat, RdfFusionServerError>,
    query_format: Result<QueryResultsFormat, RdfFusionServerError>,
) -> anyhow::Result<HandleQueryResponse> {
    let response = match query_result {
        QueryResults::Solutions(mut solutions) => {
            let format = query_format?;

            let mut buffer = Vec::new();
            let mut serializer = QueryResultsSerializer::from_format(format)
                .serialize_solutions_to_writer(&mut buffer, solutions.variables().to_vec())?;
            while let Some(solution) = solutions.next().await {
                serializer.serialize(solution?.into_iter())?;
            }
            serializer
                .finish()
                .context("Could not finalize serializer")?;

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", format.media_type())
                .body(buffer.into())
                .context("Could not build response")
        }
        QueryResults::Boolean(result) => {
            let format = query_format?;

            let mut buffer = Vec::new();
            QueryResultsSerializer::from_format(format)
                .serialize_boolean_to_writer(&mut buffer, result)?;

            Response::builder()
                .header("Content-Type", format.media_type())
                .status(StatusCode::OK)
                .body(buffer.into())
                .context("Could not build response")
        }
        QueryResults::Graph(mut triples) => {
            let format = rdf_format?;

            let mut buffer = Vec::new();
            let serializer = RdfSerializer::from_format(format);
            let mut serializer = serializer.for_writer(&mut buffer);

            while let Some(triple) = triples.next().await {
                serializer.serialize_triple(triple?.as_ref())?;
            }

            serializer
                .finish()
                .context("Could not finalize serializer")?;

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", format.media_type())
                .body(buffer.into())
                .context("Could not build response")
        }
    }?;
    Ok(HandleQueryResponse::from(response))
}

/// Holds any of the possible responses from a query request.
pub enum HandleQueryResponse {
    ServiceDescription(ServiceDescription),
    QueryResults(Response),
}

impl IntoResponse for HandleQueryResponse {
    fn into_response(self) -> Response {
        match self {
            HandleQueryResponse::ServiceDescription(sd) => sd.into_response(),
            HandleQueryResponse::QueryResults(resp) => resp,
        }
    }
}

impl From<ServiceDescription> for HandleQueryResponse {
    fn from(value: ServiceDescription) -> Self {
        Self::ServiceDescription(value)
    }
}

impl From<Response> for HandleQueryResponse {
    fn from(value: Response) -> Self {
        Self::QueryResults(value)
    }
}
