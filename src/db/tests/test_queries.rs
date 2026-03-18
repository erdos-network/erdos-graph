#[cfg(test)]
mod tests {
    use crate::config::EdgeCacheConfig;
    use crate::db::client::init_datastore;
    use crate::db::queries::{GraphQueries, HelixDbQueries};
    use crate::db::schema::{
        COAUTHORED_WITH_TYPE, GraphEdge, GraphVertex, PERSON_TYPE, PUBLICATION_TYPE,
    };
    use crate::scrapers::cache::{DeduplicationCache, EdgeCacheSystem};
    use crate::scrapers::ingestion_utils::{IngestionContext, WriteOperation, flush_buffer};
    use std::collections::HashMap;
    use tempfile::TempDir;

    /// Seed a small coauthor graph into `engine` for testing.
    ///
    /// Graph layout (COAUTHORED_WITH edges are bidirectional):
    ///
    /// ```text
    ///   Paul Erdős ─ Alice ─ Bob ─ Carol
    ///   Loner  (disconnected)
    /// ```
    ///
    /// Each edge carries a `publication_ids` JSON array linking to a seeded
    /// Publication vertex (`arxiv:0001` through `arxiv:0003`).
    fn seed_test_graph(
        engine: &std::sync::Arc<helix_db::helix_engine::traversal_core::HelixGraphEngine>,
    ) {
        // Person vertices
        let erdos = GraphVertex::new(PERSON_TYPE)
            .property("name", "Paul Erdős")
            .property("erdos_number", "0")
            .property("is_erdos", "true");
        let alice = GraphVertex::new(PERSON_TYPE)
            .property("name", "Alice")
            .property("erdos_number", "1");
        let bob = GraphVertex::new(PERSON_TYPE)
            .property("name", "Bob")
            .property("erdos_number", "2");
        let carol = GraphVertex::new(PERSON_TYPE)
            .property("name", "Carol")
            .property("erdos_number", "3");
        let loner = GraphVertex::new(PERSON_TYPE).property("name", "Loner");

        // Publication vertices — looked up via the `publication_id` secondary index
        let pub1 = GraphVertex::new(PUBLICATION_TYPE)
            .property("title", "Paper A")
            .property("year", "2000")
            .property("venue", "Journal A")
            .property("publication_id", "arxiv:0001");
        let pub2 = GraphVertex::new(PUBLICATION_TYPE)
            .property("title", "Paper B")
            .property("year", "2010")
            .property("venue", "Journal B")
            .property("publication_id", "arxiv:0002");
        let pub3 = GraphVertex::new(PUBLICATION_TYPE)
            .property("title", "Paper C")
            .property("year", "2015")
            .property("venue", "Journal C")
            .property("publication_id", "arxiv:0003");

        // COAUTHORED_WITH edges (bidirectional).
        // `publication_ids` is a JSON array string, as expected by `assemble_steps`.
        let coauth_edges = [
            GraphEdge::new(alice.id, erdos.id, COAUTHORED_WITH_TYPE)
                .property("weight", 1u64)
                .property("publication_ids", r#"["arxiv:0001"]"#),
            GraphEdge::new(erdos.id, alice.id, COAUTHORED_WITH_TYPE)
                .property("weight", 1u64)
                .property("publication_ids", r#"["arxiv:0001"]"#),
            GraphEdge::new(alice.id, bob.id, COAUTHORED_WITH_TYPE)
                .property("weight", 1u64)
                .property("publication_ids", r#"["arxiv:0002"]"#),
            GraphEdge::new(bob.id, alice.id, COAUTHORED_WITH_TYPE)
                .property("weight", 1u64)
                .property("publication_ids", r#"["arxiv:0002"]"#),
            GraphEdge::new(bob.id, carol.id, COAUTHORED_WITH_TYPE)
                .property("weight", 1u64)
                .property("publication_ids", r#"["arxiv:0003"]"#),
            GraphEdge::new(carol.id, bob.id, COAUTHORED_WITH_TYPE)
                .property("weight", 1u64)
                .property("publication_ids", r#"["arxiv:0003"]"#),
        ];

        // Use tiny bloom filters so we don't allocate 100M-entry filters in tests.
        let mut ctx = IngestionContext {
            dedup_cache: DeduplicationCache::new(100),
            author_cache: HashMap::new(),
            edge_cache: EdgeCacheSystem::new(EdgeCacheConfig {
                hot_size: 10,
                warm_size: 10,
                bloom_size: 100,
            }),
            write_buffer: Vec::new(),
            pending_edge_updates: HashMap::new(),
        };

        for v in [erdos, alice, bob, carol, loner, pub1, pub2, pub3] {
            ctx.write_buffer.push(WriteOperation::CreateVertex(v));
        }
        for e in coauth_edges {
            ctx.write_buffer.push(WriteOperation::CreateEdge(e));
        }

        flush_buffer(&mut ctx, engine).expect("seed_test_graph: flush_buffer failed");
    }

    /// Create a fresh temp database, seed the test graph, and return a ready-to-use
    /// `HelixDbQueries` together with the `TempDir` guard (drop order matters: keep
    /// `_dir` alive for the entire test so the LMDB files are not deleted early).
    fn make_queries() -> (TempDir, HelixDbQueries) {
        let dir = TempDir::new().unwrap();
        let engine = init_datastore(dir.path()).unwrap();
        seed_test_graph(&engine);
        let queries = HelixDbQueries::new(engine);
        (dir, queries)
    }

    #[tokio::test]
    async fn test_lookup_person_found() {
        let (_dir, q) = make_queries();
        let info = q.lookup_person("Alice").await.expect("Alice should exist");
        assert_eq!(info.name, "Alice");
        assert_eq!(info.erdos_number, "1");
    }

    #[tokio::test]
    async fn test_lookup_person_not_found() {
        let (_dir, q) = make_queries();
        assert!(q.lookup_person("Nobody").await.is_none());
    }

    /// `normalize_author` lowercases and collapses whitespace, so "alice" should
    /// resolve to the same index key as "Alice".
    #[tokio::test]
    async fn test_lookup_person_normalises_name() {
        let (_dir, q) = make_queries();
        let info = q
            .lookup_person("alice")
            .await
            .expect("case-insensitive lookup should succeed");
        assert_eq!(info.name, "Alice");
    }

    #[tokio::test]
    async fn test_shortest_path_direct_coauthors() {
        let (_dir, q) = make_queries();
        let path = q
            .shortest_path("Alice", "Bob")
            .await
            .expect("Alice and Bob are direct coauthors");
        assert_eq!(path.len(), 2, "Alice→Bob is one hop");
        assert_eq!(path[0].name, "Alice");
        assert_eq!(path[1].name, "Bob");
        // Edge from Alice should carry arxiv:0002 paper details.
        assert_eq!(path[0].papers_to_next.len(), 1);
        assert_eq!(path[0].papers_to_next[0].publication_id, "arxiv:0002");
        assert_eq!(path[0].papers_to_next[0].title, "Paper B");
        assert_eq!(path[0].papers_to_next[0].year, "2010");
        assert_eq!(path[0].papers_to_next[0].venue, "Journal B");
        // Final node has no outgoing papers in the path.
        assert!(path[1].papers_to_next.is_empty());
    }

    #[tokio::test]
    async fn test_shortest_path_two_hops() {
        let (_dir, q) = make_queries();
        let path = q
            .shortest_path("Alice", "Carol")
            .await
            .expect("Alice→Bob→Carol path exists");
        assert_eq!(path.len(), 3, "Alice→Carol is two hops via Bob");
        assert_eq!(path[0].name, "Alice");
        assert_eq!(path[1].name, "Bob");
        assert_eq!(path[2].name, "Carol");
        assert!(path[2].papers_to_next.is_empty());
    }

    #[tokio::test]
    async fn test_shortest_path_disconnected_returns_none() {
        let (_dir, q) = make_queries();
        assert!(q.shortest_path("Loner", "Alice").await.is_none());
    }

    #[tokio::test]
    async fn test_shortest_path_unknown_person_returns_none() {
        let (_dir, q) = make_queries();
        assert!(q.shortest_path("Ghost", "Alice").await.is_none());
    }

    /// Paul Erdős himself has Erdős number 0 — the path contains only him.
    #[tokio::test]
    async fn test_erdos_path_distance_zero() {
        let (_dir, q) = make_queries();
        let path = q
            .erdos_path("Paul Erdős")
            .await
            .expect("Paul Erdős should have distance 0");
        assert_eq!(path.len(), 1);
        assert_eq!(path[0].name, "Paul Erdős");
        assert!(path[0].papers_to_next.is_empty());
    }

    /// Alice co-authored directly with Erdős, so her Erdős number is 1.
    #[tokio::test]
    async fn test_erdos_path_distance_one() {
        let (_dir, q) = make_queries();
        let path = q
            .erdos_path("Alice")
            .await
            .expect("Alice co-authored with Erdős");
        assert_eq!(path.len(), 2, "Alice has Erdős number 1");
        assert_eq!(path[0].name, "Alice");
        assert_eq!(path[1].name, "Paul Erdős");
        // The connecting edge carries arxiv:0001.
        assert_eq!(path[0].papers_to_next.len(), 1);
        assert_eq!(path[0].papers_to_next[0].publication_id, "arxiv:0001");
        assert_eq!(path[0].papers_to_next[0].title, "Paper A");
    }

    /// Bob co-authored with Alice who co-authored with Erdős → Erdős number 2.
    #[tokio::test]
    async fn test_erdos_path_distance_two() {
        let (_dir, q) = make_queries();
        let path = q.erdos_path("Bob").await.expect("Bob has Erdős number 2");
        assert_eq!(path.len(), 3, "Bob has Erdős number 2");
        assert_eq!(path[0].name, "Bob");
        assert_eq!(path[2].name, "Paul Erdős");
        assert!(path[2].papers_to_next.is_empty());
    }

    #[tokio::test]
    async fn test_erdos_path_disconnected_returns_none() {
        let (_dir, q) = make_queries();
        assert!(q.erdos_path("Loner").await.is_none());
    }

    /// Calling `erdos_path` twice for the same person should return identical
    /// results (the second call is served from the LRU cache).
    #[tokio::test]
    async fn test_erdos_path_result_is_cached() {
        let (_dir, q) = make_queries();
        let path1 = q.erdos_path("Alice").await.expect("first call");
        let path2 = q
            .erdos_path("Alice")
            .await
            .expect("second call (from cache)");
        assert_eq!(path1.len(), path2.len());
        assert_eq!(path1[0].name, path2[0].name);
        assert_eq!(
            path1[0].papers_to_next[0].publication_id,
            path2[0].papers_to_next[0].publication_id,
        );
    }
}
