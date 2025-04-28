use crate::sparql::query::handle_query_get;
use crate::AppState;
use axum::routing::get;
use axum::Router;

mod content_negotiation;
mod query;
mod query_results;
mod service_description;
mod sparql_query_params;

pub fn create_sparql_routes() -> Router<AppState> {
    Router::new().route("/query", get(handle_query_get))
}
