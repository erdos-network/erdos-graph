//! Handler for `GET /path?from={name1}&to={name2}`
//!
//! Finds the shortest coauthorship path between two researchers and returns
//! the full path with each connecting paper's details.

use crate::server::AppState;
use crate::db::queries::PathStep;
use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ShortestPathParams {
    pub from: String,
    pub to: String,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum ShortestPathResponse {
    Found {
        found: bool,
        distance: usize,
        path: Vec<PathStep>,
    },
    NotFound {
        found: bool,
    },
}

/// `GET /path?from={name1}&to={name2}`
pub async fn shortest_path(
    State(_state): State<Arc<AppState>>,
    Query(_params): Query<ShortestPathParams>,
) -> Json<ShortestPathResponse> {
    todo!("shortest_path: normalize both names, resolve ids, spawn_blocking BFS, build_path_steps")
}
