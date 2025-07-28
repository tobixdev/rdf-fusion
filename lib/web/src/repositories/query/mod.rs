mod evaluate;
mod results;

use crate::AppState;
use crate::error::RdfFusionServerError;
use crate::repositories::query::evaluate::evaluate_sparql_query;
use crate::repositories::query::results::HandleQueryResponse;
use crate::repositories::service_description::{
    EndpointKind, generate_service_description,
};
use crate::repositories::sparql_query_params::SparqlQueryParams;
use axum::extract::State;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::results::QueryResultsFormat;

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

    evaluate_sparql_query(&state.store, &query_params, query, rdf_format, query_format)
        .await
}
