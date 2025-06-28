use axum::body::{Body, HttpBody};
use axum::extract::rejection::LengthLimitError;
use axum::extract::DefaultBodyLimit;
use axum::http::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::{middleware, routing::get, Router};
use futures::future::err;
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use tower_http::trace::{HttpMakeClassifier, TraceLayer};
use tracing::{error, Level};

mod app;
mod config;
mod error;
mod repositories;

use crate::app::create_app_routes;
use crate::repositories::create_repositories_routes;
pub use config::ServerConfig;
use rdf_fusion::store::Store;

// TODO: proper logging
#[allow(clippy::print_stdout)]
pub async fn serve(config: ServerConfig) -> anyhow::Result<()> {
    let addr = SocketAddr::from_str(&config.bind)?;

    let app_state = AppState {
        store: config.store,
        read_only: config.read_only,
        union_default_graph: config.union_default_graph,
    };

    let app = Router::new()
        .route("/", get(|| async { Redirect::permanent("/app") }))
        .nest("/app", create_app_routes())
        .nest("/repositories", create_repositories_routes())
        .with_state(app_state)
        .layer(DefaultBodyLimit::disable())
        .layer(create_tracing_layer())
        .layer(middleware::from_fn(log_error_responses));

    let app = if config.cors {
        // TODO: check how permissive this should be
        app.layer(tower_http::cors::CorsLayer::permissive())
    } else {
        app
    };

    println!("Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    Ok(axum::serve(listener, app).await?)
}

/// Creates the tracing (logging) layer for the web application.
fn create_tracing_layer() -> TraceLayer<HttpMakeClassifier> {
    TraceLayer::new_for_http()
        .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(Level::INFO))
        .on_request(tower_http::trace::DefaultOnRequest::new().level(Level::DEBUG))
        .on_response(tower_http::trace::DefaultOnResponse::new().level(Level::DEBUG))
        .on_failure(tower_http::trace::DefaultOnFailure::new().level(Level::ERROR))
}

/// Logs the body of an error response.
pub async fn log_error_responses(req: Request<Body>, next: Next) -> impl IntoResponse
{
    let response = next.run(req).await;
    let status = response.status();

    if status.is_client_error() || status.is_server_error() {
        let (parts, body) = response.into_parts();
        let body_result = axum::body::to_bytes(body, usize::MAX).await;

        match body_result {
            Ok(bytes) => {
                let body_text = String::from_utf8_lossy(bytes.as_ref());
                error!("Error response {}: {}", status, body_text);
                Response::from_parts(parts, Body::from(bytes))
            }
            Err(error) => {
                error!(
                    "Error response {}: <Could not read body>, {}",
                    status, error
                );
                Response::from_parts(parts, Body::empty())
            }
        }
    } else {
        response
    }
}

#[derive(Clone)]
struct AppState {
    store: Store,
    #[allow(unused, reason = "Not yet implemented")]
    read_only: bool,
    union_default_graph: bool,
}
