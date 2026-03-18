//! Read-only graph query utilities.
//!
//! Provides thin wrappers over the HelixDB traversal API for the HTTP server:
//! - Person existence and lookup by name
//! - Shortest path between two people (BFS via HelixDB's built-in algorithm)
//! - Publication detail lookup by source ID
//! - Path step assembly (person + connecting papers)

use serde::Serialize;

/// A resolved person from the graph.
#[derive(Debug, Clone, Serialize)]
pub struct PersonInfo {
    pub name: String,
    pub erdos_number: String,
}

/// A resolved publication from the graph.
#[derive(Debug, Clone, Serialize)]
pub struct PaperInfo {
    pub title: String,
    pub year: String,
    pub venue: String,
    pub publication_id: String,
}

/// One step in a path response: a person and the papers connecting them to the next person.
/// The last step in a path always has an empty `papers_to_next`.
#[derive(Debug, Clone, Serialize)]
pub struct PathStep {
    pub name: String,
    pub papers_to_next: Vec<PaperInfo>,
}

/// Look up a Person vertex by normalized name.
/// Returns `None` if the person is not in the graph.
///
/// # Arguments
/// * `engine` - shared HelixDB engine
/// * `person_index` - in-memory map of `normalize_author(name) -> node_id`
/// * `name` - raw name string (will be normalized internally)
pub fn lookup_person(
    _engine: &helix_db::helix_engine::traversal_core::HelixGraphEngine,
    _person_index: &std::collections::HashMap<String, u128>,
    _name: &str,
) -> Option<(u128, PersonInfo)> {
    todo!("lookup_person: secondary-index scan via person_index map")
}

/// Look up Publication vertices by their source IDs (e.g. `"arxiv:2001.12345"`).
/// Returns details for each ID that resolves to a Publication vertex.
///
/// # Arguments
/// * `engine` - shared HelixDB engine
/// * `pub_ids` - list of source-specific publication ID strings
pub fn lookup_publications_by_ids(
    _engine: &helix_db::helix_engine::traversal_core::HelixGraphEngine,
    _pub_ids: &[String],
) -> Vec<PaperInfo> {
    todo!("lookup_publications_by_ids: n_from_index on publication_id secondary index")
}

/// Find the shortest path between two Person vertices using HelixDB's built-in BFS.
/// Returns the ordered list of `(person_node_id, coauthored_with_edge_id)` pairs,
/// where the last entry has a dummy edge id of 0 (no outgoing edge from the destination).
///
/// Returns `None` if no path exists or either ID is invalid.
///
/// # Arguments
/// * `engine` - shared HelixDB engine
/// * `from_id` - source Person node id
/// * `to_id` - destination Person node id
pub fn bfs_shortest_path(
    _engine: &helix_db::helix_engine::traversal_core::HelixGraphEngine,
    _from_id: u128,
    _to_id: u128,
) -> Option<Vec<(u128, u128)>> {
    todo!("bfs_shortest_path: G::new().n_from_id().shortest_path_with_algorithm(BFS)")
}

/// Convert a raw BFS path (node/edge id pairs) into the `PathStep` response format,
/// fetching full person and publication details for each step.
///
/// # Arguments
/// * `engine` - shared HelixDB engine
/// * `path` - output of `bfs_shortest_path`
pub fn build_path_steps(
    _engine: &helix_db::helix_engine::traversal_core::HelixGraphEngine,
    _path: Vec<(u128, u128)>,
) -> Vec<PathStep> {
    todo!("build_path_steps: fetch PersonInfo per node, PaperInfo per edge's publication_ids")
}
