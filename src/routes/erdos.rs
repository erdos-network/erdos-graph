//! Handler for `GET /erdos?name={name}`
//!
//! Computes the Erdős number of a researcher: the shortest coauthorship
//! distance from them to Paul Erdős, with the full connecting path.

use crate::server::AppState;
use crate::db::queries::PathStep;
use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct ErdosParams {
    pub name: String,
}

#[derive(Serialize)]
pub struct ErdosResponse {
    /// The Erdős number, or `null` if the person has no path to Paul Erdős.
    pub erdos_number: Option<usize>,
    /// Full path from the person to Paul Erdős, or empty if not connected.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub path: Vec<PathStep>,
}

/// `GET /erdos?name={name}`
pub async fn erdos_number(
    State(_state): State<Arc<AppState>>,
    Query(_params): Query<ErdosParams>,
) -> Json<ErdosResponse> {
    todo!("erdos_number: normalize name, resolve id, spawn_blocking BFS from person to erdos_id, build_path_steps")
}
