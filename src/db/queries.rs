//! Read-only graph query interface for the HTTP server.
//!
//! Defines the [`GraphQueries`] trait which abstracts all graph reads the
//! server needs. The production implementation is [`HelixDbQueries`]; tests
//! use a lightweight mock defined in `routes::test_utils`.

use async_trait::async_trait;
use bumpalo::Bump;
use helix_db::{
    helix_engine::{
        storage_core::{HelixGraphStorage, storage_methods::StorageMethods},
        traversal_core::{
            HelixGraphEngine,
            ops::{
                g::G,
                source::n_from_id::NFromIdAdapter,
                util::paths::{PathAlgorithm, ShortestPathAdapter},
            },
            traversal_value::TraversalValue,
        },
    },
    protocol::value::Value as HelixValue,
    utils::items::{Edge, Node},
};
use lru::LruCache;
use serde::Serialize;
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use crate::db::schema::COAUTHORED_WITH_TYPE;
use crate::scrapers::cache::normalize_author;

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

    /// Scan the `name` secondary index to build `normalize_author(name) → node_id`.
    ///
    /// Simultaneously fetches each node (until Erdős is found) to detect the
    /// `is_erdos == "true"` property. Node fetches stop as soon as Erdős is
    /// located, so the cost is O(k) fetches where k is Erdős's position in the
    /// index iteration order.
    fn build_index(db: &Arc<HelixGraphEngine>) -> (HashMap<String, u128>, Option<u128>) {
        let Ok(txn) = db.storage.graph_env.read_txn() else {
            return (HashMap::new(), None);
        };
        let arena = Bump::new();

        let mut person_index = HashMap::new();
        let mut erdos_id: Option<u128> = None;

        let Some((name_index_db, _)) = db.storage.secondary_indices.get("name") else {
            return (HashMap::new(), None);
        };
        let Ok(iter) = name_index_db.iter(&txn) else {
            return (HashMap::new(), None);
        };

        for (key_bytes, node_id) in iter.flatten() {
            let Ok(HelixValue::String(name)) = bincode::deserialize(key_bytes) else {
                continue;
            };
            person_index.insert(normalize_author(&name), node_id);

            // Fetch the node to detect Paul Erdős; skip once found.
            if erdos_id.is_none()
                && let Ok(node) = db.storage.get_node(&txn, &node_id, &arena)
                && matches!(
                    node.get_property("is_erdos"),
                    Some(HelixValue::String(s)) if s == "true"
                )
            {
                erdos_id = Some(node_id);
            }
        }

        (person_index, erdos_id)
    }

    /// Run BFS between two node IDs using HelixDB's built-in shortest-path
    /// algorithm and assemble the result into [`PathStep`]s.
    ///
    /// Intended to be called inside `tokio::task::spawn_blocking` since
    /// `RoTxn` is `!Send`.
    fn run_bfs(db: Arc<HelixGraphEngine>, from_id: u128, to_id: u128) -> Option<Vec<PathStep>> {
        // Declare arena before txn so that arena outlives txn ('arena: 'txn).
        let arena = Bump::new();
        let txn = db.storage.graph_env.read_txn().ok()?;

        let result = G::new(&db.storage, &txn, &arena)
            .n_from_id(&from_id)
            .shortest_path_with_algorithm(
                Some(COAUTHORED_WITH_TYPE),
                None,
                Some(&to_id),
                PathAlgorithm::BFS,
                |_e, _f, _t| Ok(1.0),
            )
            .collect_to_obj()
            .ok()?;

        if let TraversalValue::Path((nodes, edges)) = result {
            Some(Self::assemble_steps(
                &db.storage,
                &txn,
                &arena,
                &nodes,
                &edges,
            ))
        } else {
            None
        }
    }

    /// Convert a BFS `(nodes, edges)` result into [`PathStep`]s.
    ///
    /// `papers_to_next` for node `i` is populated from `edges[i].publication_ids`;
    /// the final node always receives an empty `papers_to_next`.
    fn assemble_steps(
        storage: &HelixGraphStorage,
        txn: &heed3::RoTxn,
        arena: &Bump,
        nodes: &[Node],
        edges: &[Edge],
    ) -> Vec<PathStep> {
        nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let name = match node.get_property("name") {
                    Some(HelixValue::String(s)) => s.clone(),
                    _ => String::new(),
                };

                let papers_to_next = if i < edges.len() {
                    match edges[i].get_property("publication_ids") {
                        Some(HelixValue::String(s)) => {
                            let ids: Vec<String> = serde_json::from_str(s).unwrap_or_default();
                            Self::lookup_pubs(storage, txn, arena, &ids)
                        }
                        _ => vec![],
                    }
                } else {
                    vec![]
                };

                PathStep {
                    name,
                    papers_to_next,
                }
            })
            .collect()
    }

    /// Fetch [`PaperInfo`] for a list of source-specific publication ID strings
    /// (e.g. `"arxiv:2001.12345"`) using the `publication_id` secondary index.
    fn lookup_pubs(
        storage: &HelixGraphStorage,
        txn: &heed3::RoTxn,
        arena: &Bump,
        pub_ids: &[String],
    ) -> Vec<PaperInfo> {
        let Some((pub_index, _)) = storage.secondary_indices.get("publication_id") else {
            return vec![];
        };

        pub_ids
            .iter()
            .filter_map(|pub_id| {
                let key = bincode::serialize(&HelixValue::String(pub_id.clone())).ok()?;
                let node_id = pub_index.get(txn, key.as_slice()).ok()??;
                let node = storage.get_node(txn, &node_id, arena).ok()?;

                let title = match node.get_property("title") {
                    Some(HelixValue::String(s)) => s.clone(),
                    _ => return None,
                };
                let year = match node.get_property("year") {
                    Some(HelixValue::String(s)) => s.clone(),
                    _ => String::new(),
                };
                let venue = match node.get_property("venue") {
                    Some(HelixValue::String(s)) => s.clone(),
                    _ => String::new(),
                };

                Some(PaperInfo {
                    title,
                    year,
                    venue,
                    publication_id: pub_id.clone(),
                })
            })
            .collect()
    }
}

#[async_trait]
impl GraphQueries for HelixDbQueries {
    async fn lookup_person(&self, name: &str) -> Option<PersonInfo> {
        let node_id = *self.person_index.get(&normalize_author(name))?;
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let arena = Bump::new();
            let txn = db.storage.graph_env.read_txn().ok()?;
            let node = db.storage.get_node(&txn, &node_id, &arena).ok()?;

            let name = match node.get_property("name") {
                Some(HelixValue::String(s)) => s.clone(),
                _ => return None,
            };
            let erdos_number = match node.get_property("erdos_number") {
                Some(HelixValue::String(s)) => s.clone(),
                _ => "None".to_string(),
            };

            Some(PersonInfo { name, erdos_number })
        })
        .await
        .ok()?
    }

    async fn shortest_path(&self, from_name: &str, to_name: &str) -> Option<Vec<PathStep>> {
        let from_norm = normalize_author(from_name);
        let to_norm = normalize_author(to_name);
        let key = format!("path:{}|{}", from_norm, to_norm);

        if let Ok(mut c) = self.cache.lock()
            && let Some(cached) = c.get(&key)
        {
            return Some(cached.clone());
        }

        let from_id = *self.person_index.get(&from_norm)?;
        let to_id = *self.person_index.get(&to_norm)?;
        let db = Arc::clone(&self.db);

        let result = tokio::task::spawn_blocking(move || Self::run_bfs(db, from_id, to_id))
            .await
            .ok()??;

        if let Ok(mut c) = self.cache.lock() {
            c.put(key, result.clone());
        }

        Some(result)
    }

    async fn erdos_path(&self, name: &str) -> Option<Vec<PathStep>> {
        let erdos_id = self.erdos_id?;
        let norm = normalize_author(name);
        let key = format!("erdos:{}", norm);

        if let Ok(mut c) = self.cache.lock()
            && let Some(cached) = c.get(&key)
        {
            return Some(cached.clone());
        }

        let person_id = *self.person_index.get(&norm)?;

        // If the person IS Erdős, return a single-step path immediately.
        if person_id == erdos_id {
            let name_str = self
                .lookup_person(name)
                .await
                .map(|p| p.name)
                .unwrap_or_else(|| name.to_string());
            return Some(vec![PathStep {
                name: name_str,
                papers_to_next: vec![],
            }]);
        }

        let db = Arc::clone(&self.db);
        let result = tokio::task::spawn_blocking(move || Self::run_bfs(db, person_id, erdos_id))
            .await
            .ok()??;

        if let Ok(mut c) = self.cache.lock() {
            c.put(key, result.clone());
        }

        Some(result)
    }
}
