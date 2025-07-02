use crate::repositories::data::handle_data_post;
use crate::repositories::query::handle_query_get;
use crate::AppState;
use axum::routing::{get, post};
use axum::Router;

mod content_negotiation;
mod data;
mod query;
mod service_description;
mod sparql_query_params;

pub fn create_repositories_routes() -> Router<AppState> {
    Router::new()
        .route("/default/query", get(handle_query_get))
        .route("/default/data", post(handle_data_post))
}
