use axum::Router;
use crate::AppState;

pub fn create_app_routes() -> Router<AppState> {
    Router::new()
}