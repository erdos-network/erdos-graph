//! HTTP server setup and shared application state.
//!
//! Exposes `start_server()` which:
//!   1. Builds the in-memory person index from the name secondary index
//!   2. Locates Paul Erdős's vertex ID (used by the /erdos endpoint)
//!   3. Initializes the LRU result cache
//!   4. Mounts all routes and binds the axum server

use crate::routes::{erdos::erdos_number, path::shortest_path, person::person_exists};
use axum::{Router, routing::get};
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use lru::LruCache;
use std::{
    collections::HashMap,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};
use tower_http::cors::CorsLayer;

/// Shared state injected into every route handler via axum's `State` extractor.
pub struct AppState {
    /// The HelixDB graph engine (LMDB-backed). Shared across all handler tasks.
    pub db: Arc<HelixGraphEngine>,
    /// In-memory index: `normalize_author(name) -> node_id (u128)`.
    /// Populated once at startup by scanning the `name` secondary index.
    pub person_index: Arc<HashMap<String, u128>>,
    /// Paul Erdős's node ID, cached at startup. `None` if not present in the DB.
    pub erdos_id: Option<u128>,
    /// LRU cache for computed path/Erdős responses, keyed by canonical query string.
    /// Avoids re-running BFS for repeated requests.
    pub path_cache: Mutex<LruCache<String, serde_json::Value>>,
}

/// Build the in-memory person index and locate Paul Erdős's vertex ID.
///
/// Scans the `name` secondary index, building a `normalize_author(name) -> node_id`
/// map. While scanning, checks each vertex for `is_erdos == "true"` to capture
/// Erdős's node ID.
fn build_person_index(
    _db: &Arc<HelixGraphEngine>,
) -> (HashMap<String, u128>, Option<u128>) {
    todo!("build_person_index: read_txn, prefix_iter over name secondary index, normalize_author")
}

/// Start the HTTP server on the given port.
///
/// This function does not return unless the server errors out.
pub async fn start_server(
    port: u16,
    db: Arc<HelixGraphEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (person_index, erdos_id) = build_person_index(&db);

    let state = Arc::new(AppState {
        db,
        person_index: Arc::new(person_index),
        erdos_id,
        path_cache: Mutex::new(LruCache::new(NonZeroUsize::new(10_000).unwrap())),
    });

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
