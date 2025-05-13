use crate::sparql::create_sparql_routes;
use axum::response::Redirect;
use axum::{routing::get, Router};
use std::net::SocketAddr;
use std::str::FromStr;

mod app;
mod config;
mod error;
mod sparql;

use crate::app::create_app_routes;
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
        .nest("/sparql", create_sparql_routes())
        .with_state(app_state);

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

#[derive(Clone)]
struct AppState {
    store: Store,
    #[allow(unused, reason = "Not yet implemented")]
    read_only: bool,
    union_default_graph: bool,
}
