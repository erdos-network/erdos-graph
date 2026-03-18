//! Handler for `GET /person/exists?name={name}`
//!
//! Returns whether a Person vertex exists in the graph for the given name,
//! along with their stored metadata if found.

use crate::server::AppState;
use axum::{
    Json,
    extract::{Query, State},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
pub struct PersonExistsParams {
    pub name: String,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum PersonExistsResponse {
    Found {
        exists: bool,
        person: PersonDetails,
    },
    NotFound {
        exists: bool,
    },
}

#[derive(Serialize)]
pub struct PersonDetails {
    pub name: String,
    pub erdos_number: String,
}

/// `GET /person/exists?name={name}`
pub async fn person_exists(
    State(_state): State<Arc<AppState>>,
    Query(_params): Query<PersonExistsParams>,
) -> Json<PersonExistsResponse> {
    todo!("person_exists: normalize name, lookup in person_index, return Found/NotFound")
}
