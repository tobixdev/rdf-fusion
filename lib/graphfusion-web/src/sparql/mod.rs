use crate::sparql::query::handle_query_get;
use crate::AppState;
use axum::routing::get;
use axum::Router;

mod content_negotiation;
mod query;
mod service_description;
mod sparql_query_params;
mod query_results;

pub fn create_sparql_routes() -> Router<AppState> {
    Router::new().route("/query", get(handle_query_get))
}
