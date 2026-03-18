//! Handler for `GET /erdos?name={name}`
//!
//! Computes the Erdős number of a researcher: the shortest coauthorship
//! distance from them to Paul Erdős, with the full connecting path.

use crate::db::queries::PathStep;
use crate::server::AppState;
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
    /// Full path from the person to Paul Erdős. Empty when `erdos_number` is `null`.
    pub path: Vec<PathStep>,
}

/// `GET /erdos?name={name}`
///
/// Name normalisation, BFS, and path assembly are delegated to [`GraphQueries::erdos_path`].
/// The Erdős number is derived as `path.len() - 1`.
pub async fn erdos_number(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ErdosParams>,
) -> Json<ErdosResponse> {
    match state.queries.erdos_path(&params.name).await {
        Some(path) => {
            let number = path.len().saturating_sub(1);
            Json(ErdosResponse {
                erdos_number: Some(number),
                path,
            })
        }
        None => Json(ErdosResponse {
            erdos_number: None,
            path: vec![],
        }),
    }
}
