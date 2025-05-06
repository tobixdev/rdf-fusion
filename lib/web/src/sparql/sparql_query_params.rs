use crate::error::GraphFusionServerError;
use crate::AppState;
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use axum::RequestPartsExt;
use graphfusion::QueryOptions;
use serde::Deserialize;

#[derive(Deserialize)]
struct SparqlQueryParamsRaw {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    #[serde(rename = "using-union-graph")]
    using_union_graph: Option<bool>,
    #[serde(default)]
    #[serde(rename = "using-graph-uri")]
    using_graph_uri: Vec<String>,
    #[serde(default)]
    #[serde(rename = "using-named-graph-uri")]
    using_named_graph_uri: Vec<String>,
}

pub struct SparqlQueryParams {
    pub query: Option<String>,
    pub base_uri: String,
    pub default_graph_uris: Vec<String>,
    pub named_graph_uris: Vec<String>,
    pub default_graph_as_union: bool,
}

impl SparqlQueryParams {
    #[allow(clippy::unused_self, reason = "Self does not yet contain options.")]
    pub fn to_query_options(&self) -> QueryOptions {
        QueryOptions
    }
}

impl FromRequestParts<AppState> for SparqlQueryParams {
    type Rejection = GraphFusionServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let raw_params = parts
            .extract::<Query<SparqlQueryParamsRaw>>()
            .await
            .map_err(|e| GraphFusionServerError::BadRequest(e.to_string()))?
            .0;

        let use_default_graph_as_union = raw_params
            .using_union_graph
            .unwrap_or(state.union_default_graph);
        if use_default_graph_as_union
            && (!raw_params.using_graph_uri.is_empty()
                || !raw_params.using_named_graph_uri.is_empty())
        {
            return Err(GraphFusionServerError::BadRequest(
                "default-graph-uri or named-graph-uri and union-default-graph should not be set at the same time".to_owned()
            ));
        }

        let result = SparqlQueryParams {
            query: raw_params.query,
            base_uri: parts.uri.to_string(),
            named_graph_uris: raw_params.using_named_graph_uri,
            default_graph_uris: raw_params.using_graph_uri,
            default_graph_as_union: use_default_graph_as_union,
        };
        Ok(result)
    }
}
