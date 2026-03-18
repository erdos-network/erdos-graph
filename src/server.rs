//! HTTP server setup and shared application state.
//!
//! Exposes [`start_server`] which constructs [`AppState`], mounts all routes,
//! and binds the axum listener. The heavy lifting (index building, BFS, caching)
//! lives inside [`HelixDbQueries`].

use crate::db::queries::{GraphQueries, HelixDbQueries};
use crate::routes::{erdos::erdos_number, path::shortest_path, person::person_exists};
use axum::{Router, routing::get};
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use std::{net::SocketAddr, sync::Arc};
use tower_http::cors::CorsLayer;

/// Shared state injected into every route handler via axum's [`State`] extractor.
pub struct AppState {
    pub queries: Arc<dyn GraphQueries>,
}

/// Start the HTTP server on the given port.
///
/// Builds [`HelixDbQueries`] (which scans the DB index at startup), mounts
/// all routes, and serves until an error occurs or the process is killed.
pub async fn start_server(
    port: u16,
    db: Arc<HelixGraphEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let queries = Arc::new(HelixDbQueries::new(db));
    let state = Arc::new(AppState { queries });

    let app = Router::new()
        .route("/person/exists", get(person_exists))
        .route("/path", get(shortest_path))
        .route("/erdos", get(erdos_number))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Listening on http://{addr}");
    axum::serve(listener, app).await?;

    Ok(())
}
