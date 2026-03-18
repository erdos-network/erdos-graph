//! Shared test utilities for route handler unit tests.
//!
//! Provides [`MockGraphQueries`] — a configurable stand-in for [`GraphQueries`]
//! that returns preset values without touching the database — and [`make_state`]
//! for wiring it into an [`AppState`].

use crate::db::queries::{GraphQueries, PathStep, PersonInfo};
use crate::server::AppState;
use async_trait::async_trait;
use std::sync::Arc;

/// Configurable mock for [`GraphQueries`].
///
/// Set each field to the value you want the corresponding method to return.
/// `None` simulates the "not found / no path" case.
pub(crate) struct MockGraphQueries {
    /// Returned by [`lookup_person`] regardless of the name queried.
    pub person: Option<PersonInfo>,
    /// Returned by both [`shortest_path`] and [`erdos_path`] regardless of the names queried.
    pub path: Option<Vec<PathStep>>,
}

#[async_trait]
impl GraphQueries for MockGraphQueries {
    async fn lookup_person(&self, _name: &str) -> Option<PersonInfo> {
        self.person.clone()
    }

    async fn shortest_path(&self, _from: &str, _to: &str) -> Option<Vec<PathStep>> {
        self.path.clone()
    }

    async fn erdos_path(&self, _name: &str) -> Option<Vec<PathStep>> {
        self.path.clone()
    }
}

/// Wrap a [`MockGraphQueries`] in an [`AppState`] ready to pass to a test router.
pub(crate) fn make_state(mock: MockGraphQueries) -> Arc<AppState> {
    Arc::new(AppState {
        queries: Arc::new(mock),
    })
}
