//! Read-only graph query interface for the HTTP server.
//!
//! Defines the [`GraphQueries`] trait which abstracts all graph reads the
//! server needs. The production implementation is [`HelixDbQueries`]; tests
//! use a lightweight mock defined in `routes::test_utils`.

use async_trait::async_trait;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use lru::LruCache;
use serde::Serialize;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

/// A resolved Person vertex from the graph.
#[derive(Debug, Clone, Serialize)]
pub struct PersonInfo {
    pub name: String,
    /// The stored Erdős-number string, e.g. `"3"` or `"None"` if uncomputed.
    pub erdos_number: String,
}

/// A resolved Publication vertex from the graph.
#[derive(Debug, Clone, Serialize)]
pub struct PaperInfo {
    pub title: String,
    pub year: String,
    pub venue: String,
    /// Source-specific identifier, e.g. `"arxiv:2001.12345"`.
    pub publication_id: String,
}

/// One step in a path response: a person and the papers connecting them to the
/// next person in the path. The final step always has an empty `papers_to_next`.
#[derive(Debug, Clone, Serialize)]
pub struct PathStep {
    pub name: String,
    pub papers_to_next: Vec<PaperInfo>,
}

/// Abstracts all graph read operations required by the HTTP server.
///
/// Implemented by [`HelixDbQueries`] in production and by `MockGraphQueries`
/// in unit tests.
#[async_trait]
pub trait GraphQueries: Send + Sync {
    /// Look up a person by name (applies `normalize_author` internally).
    /// Returns `None` if no matching Person vertex exists.
    async fn lookup_person(&self, name: &str) -> Option<PersonInfo>;

    /// Shortest coauthorship path between two people (by raw name).
    ///
    /// Normalises both names, resolves them to node IDs, runs BFS via
    /// HelixDB's built-in shortest-path algorithm, then assembles the result
    /// into [`PathStep`]s including full paper details per edge.
    ///
    /// Returns `None` if either name is unknown or no path connects them.
    async fn shortest_path(&self, from_name: &str, to_name: &str) -> Option<Vec<PathStep>>;

    /// Shortest path from the named person to Paul Erdős.
    ///
    /// Returns `None` if the person is unknown or has no connection to Erdős.
    async fn erdos_path(&self, name: &str) -> Option<Vec<PathStep>>;
}

/// Production [`GraphQueries`] implementation backed by HelixDB (LMDB).
pub struct HelixDbQueries {
    db: Arc<HelixGraphEngine>,
    /// `normalize_author(name)` → node id; built once at startup.
    person_index: HashMap<String, u128>,
    /// Paul Erdős's node ID, or `None` if not present in the DB.
    erdos_id: Option<u128>,
    /// Memoised path results keyed by canonical query string (e.g. `"path:alice|bob"`).
    cache: Mutex<LruCache<String, Vec<PathStep>>>,
}

impl HelixDbQueries {
    /// Construct by scanning the HelixDB name secondary index to populate the
    /// in-memory person index and locate Paul Erdős.
    pub fn new(db: Arc<HelixGraphEngine>) -> Self {
        let (person_index, erdos_id) = Self::build_index(&db);
        Self {
            db,
            person_index,
            erdos_id,
            cache: Mutex::new(LruCache::new(NonZeroUsize::new(10_000).unwrap())),
        }
    }

    /// Scan the `name` secondary index; return `(normalize_author(name) → node_id, erdos_id)`.
    fn build_index(_db: &Arc<HelixGraphEngine>) -> (HashMap<String, u128>, Option<u128>) {
        todo!("build_index: read_txn, prefix_iter over name secondary index, normalize_author, detect is_erdos == 'true'")
    }

    /// BFS shortest path between two node IDs via HelixDB's built-in algorithm.
    /// Returns an ordered list of `(person_node_id, coauthored_with_edge_id)` pairs;
    /// the last entry carries a dummy edge id of `0`.
    fn bfs_shortest_path(&self, _from_id: u128, _to_id: u128) -> Option<Vec<(u128, u128)>> {
        todo!("bfs_shortest_path: read_txn, G::new().n_from_id().shortest_path_with_algorithm(PathAlgorithm::BFS)")
    }

    /// Convert a raw BFS path into [`PathStep`]s by fetching person and publication details.
    fn build_path_steps(&self, _path: Vec<(u128, u128)>) -> Vec<PathStep> {
        todo!("build_path_steps: per node fetch PersonInfo; per edge parse publication_ids JSON and call lookup_publications_by_ids")
    }

    /// Fetch [`PaperInfo`] for each source-specific publication ID string.
    fn lookup_publications_by_ids(&self, _pub_ids: &[String]) -> Vec<PaperInfo> {
        todo!("lookup_publications_by_ids: read_txn, n_from_index on publication_id secondary index")
    }
}

#[async_trait]
impl GraphQueries for HelixDbQueries {
    async fn lookup_person(&self, _name: &str) -> Option<PersonInfo> {
        todo!("lookup_person: normalize_author, person_index lookup, fetch node properties")
    }

    async fn shortest_path(&self, from_name: &str, to_name: &str) -> Option<Vec<PathStep>> {
        let key = format!(
            "path:{}|{}",
            crate::scrapers::cache::normalize_author(from_name),
            crate::scrapers::cache::normalize_author(to_name)
        );
        if let Ok(mut c) = self.cache.lock() {
            if let Some(cached) = c.get(&key) {
                return Some(cached.clone());
            }
        }
        todo!("shortest_path: resolve IDs via person_index, spawn_blocking bfs_shortest_path, build_path_steps, populate cache")
    }

    async fn erdos_path(&self, name: &str) -> Option<Vec<PathStep>> {
        let _erdos_id = self.erdos_id?;
        let key = format!("erdos:{}", crate::scrapers::cache::normalize_author(name));
        if let Ok(mut c) = self.cache.lock() {
            if let Some(cached) = c.get(&key) {
                return Some(cached.clone());
            }
        }
        todo!("erdos_path: resolve name to node ID via person_index, spawn_blocking bfs_shortest_path(id, erdos_id), build_path_steps, populate cache")
    }
}
