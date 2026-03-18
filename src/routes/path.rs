//! Handler for `GET /path?from={name1}&to={name2}`
//!
//! Finds the shortest coauthorship path between two researchers and returns
//! the full path with each connecting paper's details.

use crate::db::queries::PathStep;
use crate::server::AppState;
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
        /// Number of edges in the path (i.e. `path.len() - 1`).
        distance: usize,
        path: Vec<PathStep>,
    },
    NotFound {
        found: bool,
    },
}

/// `GET /path?from={name1}&to={name2}`
///
/// Name normalisation and BFS are delegated to [`GraphQueries::shortest_path`].
pub async fn shortest_path(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ShortestPathParams>,
) -> Json<ShortestPathResponse> {
    match state.queries.shortest_path(&params.from, &params.to).await {
        Some(path) => {
            let distance = path.len().saturating_sub(1);
            Json(ShortestPathResponse::Found {
                found: true,
                distance,
                path,
            })
        }
        None => Json(ShortestPathResponse::NotFound { found: false }),
    }
}
