use crate::AppState;
use axum::Router;

pub fn create_app_routes() -> Router<AppState> {
    Router::new()
}
