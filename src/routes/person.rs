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

/// Serialisable person details embedded in a [`PersonExistsResponse::Found`].
#[derive(Serialize)]
pub struct PersonDetails {
    pub name: String,
    pub erdos_number: String,
}

/// `GET /person/exists?name={name}`
///
/// Normalisation of the name (trim, lowercase, collapse whitespace) is handled
/// inside [`GraphQueries::lookup_person`].
pub async fn person_exists(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PersonExistsParams>,
) -> Json<PersonExistsResponse> {
    match state.queries.lookup_person(&params.name).await {
        Some(info) => Json(PersonExistsResponse::Found {
            exists: true,
            person: PersonDetails {
                name: info.name,
                erdos_number: info.erdos_number,
            },
        }),
        None => Json(PersonExistsResponse::NotFound { exists: false }),
    }
}
