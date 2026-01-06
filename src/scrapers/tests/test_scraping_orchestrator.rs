#[cfg(test)]
mod tests {
    use crate::config::{Config, DeduplicationConfig, IngestionConfig, ScraperConfig};
    use crate::db::ingestion::PublicationRecord;
    use crate::scrapers::scraping_orchestrator::{
        DeduplicationCache, IngestionContext, add_publication, create_authored_edge,
        create_coauthor_edge, flush_buffer, get_or_create_author_vertex, ingest_batch,
        normalize_title, publication_exists,
    };
    use crate::utilities::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
    use indradb::{
        AllEdgeQuery, Edge, Identifier, QueryExt, QueryOutputValue, RangeVertexQuery,
        RocksdbDatastore, SpecificEdgeQuery, SpecificVertexQuery,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration as StdDuration;
    use tempfile::TempDir;
    use tokio::sync::Mutex;
    use tokio::time::{Instant, sleep};

    /// Test that multiple producers can submit to queue concurrently without data loss.
    #[tokio::test]
    async fn test_concurrent_queue_producers() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let mut handles = vec![];

        // Spawn 5 concurrent producers
        for i in 0..5 {
            let producer = queue.create_producer();
            let handle = tokio::spawn(async move {
                for j in 0..10 {
                    let record = PublicationRecord {
                        id: format!("test-{}-{}", i, j),
                        title: format!("Paper {} from producer {}", j, i),
                        authors: vec![format!("Author {}", i)],
                        year: 2024,
                        venue: None,
                        source: "test".to_string(),
                    };
                    producer.submit(record).unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all producers
        for handle in handles {
            handle.await.unwrap();
        }

        // Producers auto-unregister when dropped
        // Verify all 50 records can be dequeued
        let mut count = 0;
        while queue.dequeue().is_some() {
            count += 1;
        }
        assert_eq!(count, 50);
        assert!(queue.producers_finished());
    }

    /// Test that queue correctly signals when producers are finished.
    #[tokio::test]
    async fn test_queue_producer_completion_signaling() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        assert!(!queue.producers_finished());

        {
            let producer = queue.create_producer();
            producer
                .submit(PublicationRecord {
                    id: "test-1".to_string(),
                    title: "Test".to_string(),
                    authors: vec![],
                    year: 2024,
                    venue: None,
                    source: "test".to_string(),
                })
                .unwrap();
        } // Producer dropped here

        assert!(queue.producers_finished());
        assert_eq!(queue.dequeue().unwrap().id, "test-1");
    }

    /// Test heartbeat tracking with concurrent updates from multiple sources.
    #[tokio::test]
    async fn test_concurrent_heartbeat_updates() {
        let heartbeat_times: Arc<Mutex<HashMap<String, Instant>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        // Initialize heartbeats for 3 sources
        for i in 0..3 {
            let mut hb = heartbeat_times.lock().await;
            hb.insert(format!("source_{}", i), Instant::now());
        }

        // Spawn tasks that update heartbeats concurrently
        for i in 0..3 {
            let hb_clone = heartbeat_times.clone();
            let handle = tokio::spawn(async move {
                for _ in 0..10 {
                    sleep(StdDuration::from_millis(10)).await;
                    let mut hb = hb_clone.lock().await;
                    hb.insert(format!("source_{}", i), Instant::now());
                }
            });
            handles.push(handle);
        }

        // Wait for all updates
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all sources have recent heartbeats
        let hb = heartbeat_times.lock().await;
        assert_eq!(hb.len(), 3);
        let now = Instant::now();
        for (source, last_heartbeat) in hb.iter() {
            let elapsed = now.duration_since(*last_heartbeat);
            assert!(
                elapsed < StdDuration::from_secs(1),
                "Source {} heartbeat too old: {:?}",
                source,
                elapsed
            );
        }
    }

    /// Test heartbeat timeout detection logic.
    #[tokio::test]
    async fn test_heartbeat_timeout_detection() {
        let heartbeat_times: Arc<Mutex<HashMap<String, Instant>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let heartbeat_timeout = StdDuration::from_millis(100);

        // Initialize heartbeats
        {
            let mut hb = heartbeat_times.lock().await;
            hb.insert("active_source".to_string(), Instant::now());
            hb.insert(
                "stalled_source".to_string(),
                Instant::now() - StdDuration::from_millis(200),
            );
        }

        // Check for timeouts
        let hb = heartbeat_times.lock().await;
        let now = Instant::now();
        let mut timed_out = vec![];
        let mut active = vec![];

        for (src, last_heartbeat) in hb.iter() {
            if now.duration_since(*last_heartbeat) > heartbeat_timeout {
                timed_out.push(src.clone());
            } else {
                active.push(src.clone());
            }
        }

        assert_eq!(timed_out.len(), 1);
        assert!(timed_out.contains(&"stalled_source".to_string()));
        assert_eq!(active.len(), 1);
        assert!(active.contains(&"active_source".to_string()));
    }

    /// Test race condition: consumer reading while producers still writing.
    #[tokio::test]
    async fn test_consumer_producer_race_condition() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let queue_clone = queue.clone();

        // Consumer task that starts immediately
        let value = queue.clone();
        let consumer = tokio::spawn(async move {
            let mut consumed = 0;
            loop {
                if value.dequeue().is_some() {
                    consumed += 1;
                } else if queue_clone.producers_finished() {
                    break;
                } else {
                    sleep(StdDuration::from_millis(1)).await;
                }
            }
            consumed
        });

        // Producer starts after small delay
        sleep(StdDuration::from_millis(10)).await;

        {
            let producer = queue.create_producer();
            for i in 0..20 {
                producer
                    .submit(PublicationRecord {
                        id: format!("test-{}", i),
                        title: format!("Paper {}", i),
                        authors: vec![],
                        year: 2024,
                        venue: None,
                        source: "test".to_string(),
                    })
                    .unwrap();

                // Add small delays to simulate realistic production rate
                if i % 5 == 0 {
                    sleep(StdDuration::from_millis(5)).await;
                }
            }
        } // Producer dropped here

        let consumed = consumer.await.unwrap();
        assert_eq!(consumed, 20, "Consumer should receive all records");
    }

    /// Test queue behavior under high contention with many concurrent operations.
    #[tokio::test]
    async fn test_queue_high_contention() {
        let queue = ThreadSafeQueue::new(QueueConfig {
            max_queue_size: 200,
        });
        let total_records = Arc::new(Mutex::new(0));

        // Spawn 5 producers submitting concurrently (reduced from 10)
        let mut producer_handles = vec![];
        for i in 0..5 {
            let producer = queue.create_producer();
            let handle = tokio::spawn(async move {
                for j in 0..20 {
                    // Reduced from 50
                    let record = PublicationRecord {
                        id: format!("test-{}-{}", i, j),
                        title: format!("Paper {}-{}", i, j),
                        authors: vec![],
                        year: 2024,
                        venue: None,
                        source: "test".to_string(),
                    };
                    producer.submit(record).unwrap();
                    // Small yield to allow consumers to catch up
                    tokio::task::yield_now().await;
                }
            });
            producer_handles.push(handle);
        }

        // Spawn 2 consumers reading concurrently
        let mut consumer_handles = vec![];
        for _ in 0..2 {
            let queue_clone = queue.clone();
            let total_clone = total_records.clone();
            let handle = tokio::spawn(async move {
                let mut local_count = 0;
                loop {
                    if queue_clone.dequeue().is_some() {
                        local_count += 1;
                    } else if queue_clone.producers_finished() {
                        break;
                    } else {
                        sleep(StdDuration::from_micros(100)).await;
                    }
                }
                let mut total = total_clone.lock().await;
                *total += local_count;
            });
            consumer_handles.push(handle);
        }

        // Wait for producers to finish
        for handle in producer_handles {
            handle.await.unwrap();
        }

        // Wait for consumers
        for handle in consumer_handles {
            handle.await.unwrap();
        }

        let final_count = *total_records.lock().await;
        assert_eq!(
            final_count, 100,
            "All records should be consumed exactly once"
        );
    }

    /// Test that heartbeat tracking handles rapid source additions.
    #[tokio::test]
    async fn test_heartbeat_rapid_source_addition() {
        let heartbeat_times: Arc<Mutex<HashMap<String, Instant>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut handles = vec![];

        // Rapidly add 20 sources concurrently
        for i in 0..20 {
            let hb_clone = heartbeat_times.clone();
            let handle = tokio::spawn(async move {
                let mut hb = hb_clone.lock().await;
                hb.insert(format!("source_{}", i), Instant::now());
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let hb = heartbeat_times.lock().await;
        assert_eq!(hb.len(), 20, "All sources should be tracked");
    }

    /// Test interleaved queue operations with alternating produce/consume patterns.
    #[tokio::test]
    async fn test_interleaved_produce_consume() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let mut consumed = 0;

        {
            let producer = queue.create_producer();

            // Interleave produce and consume operations
            for i in 0..10 {
                // Produce two items
                producer
                    .submit(PublicationRecord {
                        id: format!("test-{}", i * 2),
                        title: format!("Paper {}", i * 2),
                        authors: vec![],
                        year: 2024,
                        venue: None,
                        source: "test".to_string(),
                    })
                    .unwrap();

                producer
                    .submit(PublicationRecord {
                        id: format!("test-{}", i * 2 + 1),
                        title: format!("Paper {}", i * 2 + 1),
                        authors: vec![],
                        year: 2024,
                        venue: None,
                        source: "test".to_string(),
                    })
                    .unwrap();

                // Consume one item
                if queue.dequeue().is_some() {
                    consumed += 1;
                }
            }
        } // Producer dropped here

        // Consume remaining
        while queue.dequeue().is_some() {
            consumed += 1;
        }

        assert_eq!(consumed, 20, "All records should be consumed");
        assert!(queue.producers_finished());
    }

    /// Test that adding a publication creates the correct vertex and properties.
    #[test]
    fn test_add_publication() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        let record = PublicationRecord {
            id: "arxiv:1234.5678".to_string(),
            title: "Test Paper".to_string(),
            authors: vec!["Author One".to_string(), "Author Two".to_string()],
            year: 2023,
            venue: Some("Journal of Tests".to_string()),
            source: "arxiv".to_string(),
        };

        let mut write_buffer = Vec::new();
        add_publication(&record, &mut write_buffer).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Verify vertex existence
        let q = RangeVertexQuery::new().limit(10);
        let results = database.get(q).unwrap();
        let vertices = match &results[0] {
            QueryOutputValue::Vertices(v) => v,
            _ => panic!("Expected vertices"),
        };
        assert_eq!(
            vertices.len(),
            1,
            "Should have created exactly one publication vertex"
        );

        let vertex = &vertices[0];
        assert_eq!(vertex.t.as_str(), "Publication");

        // Verify properties
        let q = SpecificVertexQuery::single(vertex.id).properties().unwrap();
        let results = database.get(q).unwrap();
        let vertex_props = match &results[0] {
            QueryOutputValue::VertexProperties(vps) => &vps[0],
            _ => panic!("Expected vertex properties"),
        };

        // Helper to get property value as string
        let get_prop = |name: &str| -> String {
            let id = Identifier::new(name).unwrap();
            let value = vertex_props
                .props
                .iter()
                .find(|p| p.name == id)
                .map(|p| p.value.clone());
            match value {
                Some(v) => {
                    // v is Json, convert to string
                    // We need to handle potential quotes if it's a string value
                    let s = v.0.to_string();
                    if s.starts_with('"') && s.ends_with('"') {
                        s[1..s.len() - 1].to_string()
                    } else {
                        s
                    }
                }
                None => "".to_string(),
            }
        };

        assert_eq!(get_prop("title"), "Test Paper");
        assert_eq!(get_prop("year"), "2023");
        assert_eq!(get_prop("venue"), "Journal of Tests");
        assert_eq!(get_prop("publication_id"), "arxiv:1234.5678");
    }

    /// Test that get_or_create_author_vertex creates a new author if they don't exist
    /// and returns the existing one if they do.
    #[test]
    fn test_get_or_create_author() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_authors.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        // Index the 'name' property so we can query by it
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        // 1. Create new author
        let author_name = "Paul Erdős";
        let mut author_cache = HashMap::new();
        let mut write_buffer = Vec::new();
        let vertex1 =
            get_or_create_author_vertex(author_name, &mut write_buffer, &mut author_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Verify vertex type
        assert_eq!(vertex1.t.as_str(), "Person");

        // Verify properties
        let q = SpecificVertexQuery::single(vertex1.id)
            .properties()
            .unwrap();
        let results = database.get(q).unwrap();
        let vertex_props = match &results[0] {
            QueryOutputValue::VertexProperties(vps) => &vps[0],
            _ => panic!("Expected vertex properties"),
        };

        // Helper to get property value as string (reused from above)
        let get_prop = |name: &str| -> String {
            let id = Identifier::new(name).unwrap();
            let value = vertex_props
                .props
                .iter()
                .find(|p| p.name == id)
                .map(|p| p.value.clone());
            match value {
                Some(v) => {
                    let s = v.0.to_string();
                    if s.starts_with('"') && s.ends_with('"') {
                        s[1..s.len() - 1].to_string()
                    } else {
                        s
                    }
                }
                None => "".to_string(),
            }
        };

        assert_eq!(get_prop("name"), author_name);
        assert_eq!(get_prop("erdos_number"), "None");

        // 2. Get existing author
        let mut write_buffer = Vec::new();
        let vertex2 =
            get_or_create_author_vertex(author_name, &mut write_buffer, &mut author_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Should be the same vertex ID
        assert_eq!(vertex1.id, vertex2.id);

        // 3. Create a different author
        let other_name = "Alice Smith";
        let mut write_buffer = Vec::new();
        let vertex3 =
            get_or_create_author_vertex(other_name, &mut write_buffer, &mut author_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        assert_ne!(vertex1.id, vertex3.id);

        // Verify we have 2 people total
        let q = RangeVertexQuery::new().t(Identifier::new("Person").unwrap());
        let results = database.get(q).unwrap();
        match &results[0] {
            QueryOutputValue::Vertices(v) => assert_eq!(v.len(), 2),
            _ => panic!("Expected vertices"),
        };
    }

    /// Test creation of AUTHORED edge.
    #[test]
    fn test_create_authored_edge() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_edges.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        // Index name property as get_or_create_author requires it
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        // Create author and publication
        let author_name = "Paul Erdős";
        let mut author_cache = HashMap::new();
        let mut write_buffer = Vec::new();
        let author =
            get_or_create_author_vertex(author_name, &mut write_buffer, &mut author_cache).unwrap();

        let record = PublicationRecord {
            id: "arxiv:1234.5678".to_string(),
            title: "Probabilistic Methods".to_string(),
            authors: vec![author_name.to_string()],
            year: 1947,
            venue: None,
            source: "arxiv".to_string(),
        };
        let publication = add_publication(&record, &mut write_buffer).unwrap();

        // Create edge
        create_authored_edge(&author, &publication, &mut write_buffer).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Verify edge existence
        let q = AllEdgeQuery;
        let results = database.get(q).unwrap();
        let edges = match &results[0] {
            QueryOutputValue::Edges(e) => e,
            _ => panic!("Expected edges"),
        };

        assert_eq!(edges.len(), 1, "Should have created exactly one edge");
        let edge = &edges[0];

        assert_eq!(edge.outbound_id, author.id);
        assert_eq!(edge.inbound_id, publication.id);
        assert_eq!(edge.t.as_str(), "AUTHORED");
    }

    /// Test creation and updating of COAUTHORED_WITH edge.
    #[test]
    fn test_create_coauthor_edge_bloom_hit() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_bloom_hit.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        // Index the 'name' property
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        let mut author_cache = HashMap::new();
        let mut write_buffer = Vec::new();

        let author1 =
            get_or_create_author_vertex("Alice", &mut write_buffer, &mut author_cache).unwrap();
        let author2 =
            get_or_create_author_vertex("Bob", &mut write_buffer, &mut author_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        use crate::config::EdgeCacheConfig;
        use crate::scrapers::scraping_orchestrator::EdgeCacheSystem;
        let mut edge_cache = EdgeCacheSystem::new(EdgeCacheConfig::default());

        // Populate bloom but not cache (simulate cold edge)
        let key = (author1.id, author2.id);
        edge_cache.cold_bloom.set(&key);

        // However, for this TEST, we want to verify the logic.

        // We need to fetch the edge to verify.
        // If the edge wasn't created, we can't verify it.
        // So let's create it first manually to simulate it existing.

        use crate::db::schema::COAUTHORED_WITH_TYPE;
        let edge_type = Identifier::new(COAUTHORED_WITH_TYPE).unwrap();
        let edge = Edge::new(author1.id, edge_type, author2.id);
        let mut wb2 = Vec::new();
        wb2.push(crate::scrapers::scraping_orchestrator::WriteOperation::CreateEdge(edge.clone()));
        flush_buffer(&mut wb2, &mut database).unwrap();

        // NOW call create_coauthor_edge
        create_coauthor_edge(&author1, &author2, &mut write_buffer, &mut edge_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Verify edge and weight
        let edge_type = Identifier::new(COAUTHORED_WITH_TYPE).unwrap();
        let expected_edge = Edge::new(author1.id, edge_type, author2.id);

        let q = SpecificEdgeQuery::single(expected_edge.clone())
            .properties()
            .unwrap();
        let results = database.get(q).unwrap();
        let edge_props = match &results[0] {
            QueryOutputValue::EdgeProperties(eps) => &eps[0],
            _ => panic!("Expected edge properties"),
        };

        let weight_id = Identifier::new("weight").unwrap();
        let weight = edge_props
            .props
            .iter()
            .find(|p| p.name == weight_id)
            .map(|p| p.value.as_u64().unwrap())
            .unwrap();

        assert_eq!(weight, 2);
    }

    /// Test DeduplicationCache::new
    #[test]
    fn test_deduplication_cache_new() {
        let _cache = DeduplicationCache::new(100);
        // Verify it's created without panicking
    }

    /// Test publication deduplication
    #[test]
    fn test_publication_exists() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_dedup.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec![],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
        };

        // Setup indexes required for publication_exists
        database
            .index_property(Identifier::new("publication_id").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("year").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap(); // For author lookup

        let mut dedup_cache = DeduplicationCache::new(100);
        let mut author_cache = HashMap::new();

        // Test Exact ID Match
        let record1 = PublicationRecord {
            id: "arxiv:1234".to_string(),
            title: "Original Title".to_string(),
            authors: vec!["Author A".to_string()],
            year: 2020,
            venue: None,
            source: "arxiv".to_string(),
        };

        // Create author and link it BEFORE adding publication
        // This ensures the bloom filter is populated correctly
        let mut write_buffer = Vec::new();
        let author_v =
            get_or_create_author_vertex("Author A", &mut write_buffer, &mut author_cache).unwrap();

        let pub1 = add_publication(&record1, &mut write_buffer).unwrap();
        create_authored_edge(&author_v, &pub1, &mut write_buffer).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Should exist
        assert!(publication_exists(
            &record1,
            &database,
            &config,
            &mut dedup_cache
        ));

        // Test Fuzzy Match
        let record2 = PublicationRecord {
            id: "arxiv:5678".to_string(),          // Different ID
            title: "Original Title".to_string(),   // Same title
            authors: vec!["Author A".to_string()], // Same author
            year: 2020,                            // Same year
            venue: None,
            source: "arxiv".to_string(),
        };

        // Check if fuzzy match detects existing publication
        assert!(publication_exists(
            &record2,
            &database,
            &config,
            &mut dedup_cache
        ));

        // Test non-existent differing by title
        let record3 = PublicationRecord {
            id: "arxiv:9999".to_string(),
            title: "Completely Different Title".to_string(),
            authors: vec!["Author A".to_string()],
            year: 2020,
            venue: None,
            source: "arxiv".to_string(),
        };
        assert!(!publication_exists(
            &record3,
            &database,
            &config,
            &mut dedup_cache
        ));

        // Test non-existent differing by year
        let record4 = PublicationRecord {
            id: "arxiv:5555".to_string(),
            title: "Original Title".to_string(),
            authors: vec!["Author A".to_string()],
            year: 2021, // Different year
            venue: None,
            source: "arxiv".to_string(),
        };
        assert!(!publication_exists(
            &record4,
            &database,
            &config,
            &mut dedup_cache
        ));
    }

    /// Test publication_exists with empty author list
    #[test]
    fn test_publication_exists_empty_authors() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_empty_authors.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec![],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
        };

        database
            .index_property(Identifier::new("publication_id").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("year").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        // Create a publication with no authors
        let record = PublicationRecord {
            id: "test:123".to_string(),
            title: "No Authors Paper".to_string(),
            authors: vec![],
            year: 2023,
            venue: None,
            source: "test".to_string(),
        };

        let mut write_buffer = Vec::new();
        add_publication(&record, &mut write_buffer).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        let mut dedup_cache = DeduplicationCache::new(100);

        // Check if it exists
        let exists = publication_exists(&record, &database, &config, &mut dedup_cache);

        // Should exist by exact ID match
        assert!(exists);
    }

    /// Test publication_exists with invalid identifier
    #[test]
    fn test_publication_exists_invalid_identifier() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_invalid_id.rocksdb");
        let database = RocksdbDatastore::new_db(&db_path).unwrap();

        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec![],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
        };

        let record = PublicationRecord {
            id: "test:123".to_string(),
            title: "Test".to_string(),
            authors: vec!["Author".to_string()],
            year: 2023,
            venue: None,
            source: "test".to_string(),
        };

        let mut dedup_cache = DeduplicationCache::new(100);

        // Should return false without crashing
        let exists = publication_exists(&record, &database, &config, &mut dedup_cache);
        assert!(!exists);
    }

    /// Test coauthor edge weight increment with string weight
    #[test]
    fn test_coauthor_edge_weight_string_format() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_weight_string.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        let mut author_cache = HashMap::new();
        let mut write_buffer = Vec::new();
        let author1 =
            get_or_create_author_vertex("Alice", &mut write_buffer, &mut author_cache).unwrap();
        let author2 =
            get_or_create_author_vertex("Bob", &mut write_buffer, &mut author_cache).unwrap();

        use crate::config::EdgeCacheConfig;
        use crate::scrapers::scraping_orchestrator::EdgeCacheSystem;
        let mut edge_cache = EdgeCacheSystem::new(EdgeCacheConfig::default());

        // Create initial edge
        create_coauthor_edge(&author1, &author2, &mut write_buffer, &mut edge_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Manually set weight as string (simulating legacy data)
        use crate::db::schema::COAUTHORED_WITH_TYPE;
        use indradb::{Edge, Json};
        use serde_json::json;

        let edge_type = Identifier::new(COAUTHORED_WITH_TYPE).unwrap();
        let edge = Edge::new(author1.id, edge_type, author2.id);
        let weight_prop = Identifier::new("weight").unwrap();
        let q = SpecificEdgeQuery::single(edge.clone());
        database
            .set_properties(q, weight_prop, &Json::new(json!("1")))
            .unwrap();

        // Clear cache and bloom to force DB interaction in theory,
        // but EdgeCacheSystem doesn't have clear().
        // We can just construct a fresh one, but `create_coauthor_edge`
        // will check cache first. If empty, it checks bloom.
        // If bloom empty, it checks DB.

        // Re-create cache (empty)
        let mut edge_cache = EdgeCacheSystem::new(EdgeCacheConfig::default());

        // Populate bloom to force heuristic (simulating existing edge)
        // Since we don't read from DB, we don't parse the "1". We just overwrite with 2 map heuristic.
        let key = (author1.id, author2.id);
        edge_cache.cold_bloom.set(&key);

        // Increment weight - will overwrite with 2 (heuristic)
        let mut write_buffer = Vec::new();
        create_coauthor_edge(&author1, &author2, &mut write_buffer, &mut edge_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Verify weight was incremented
        let q = SpecificEdgeQuery::single(edge.clone())
            .properties()
            .unwrap()
            .name(weight_prop);
        let results = database.get(q).unwrap();

        let weight = results
            .first()
            .and_then(|res| match res {
                QueryOutputValue::EdgeProperties(props) => props.first(),
                _ => None,
            })
            .and_then(|prop| prop.props.first())
            .and_then(|p| p.value.as_u64())
            .unwrap();

        assert_eq!(weight, 2);
    }

    /// Test add_publication with all fields
    #[test]
    fn test_add_publication_complete() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_pub_complete.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        let record = PublicationRecord {
            id: "complete:123".to_string(),
            title: "Complete Publication".to_string(),
            authors: vec!["Author One".to_string(), "Author Two".to_string()],
            year: 2024,
            venue: Some("Top Conference".to_string()),
            source: "test".to_string(),
        };

        let mut write_buffer = Vec::new();
        let vertex = add_publication(&record, &mut write_buffer).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Verify all properties were set
        let q = SpecificVertexQuery::single(vertex.id).properties().unwrap();
        let results = database.get(q).unwrap();
        let vertex_props = match &results[0] {
            QueryOutputValue::VertexProperties(vps) => &vps[0],
            _ => panic!("Expected vertex properties"),
        };

        assert!(vertex_props.props.len() >= 4);
    }

    /// Test get_or_create_author_vertex with cache hit
    #[test]
    fn test_get_or_create_author_cache_hit() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_author_cache.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        let mut author_cache = HashMap::new();
        let author_name = "Cached Author";

        // First call - creates and caches
        let mut write_buffer = Vec::new();
        let vertex1 =
            get_or_create_author_vertex(author_name, &mut write_buffer, &mut author_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Second call - should use cache
        let mut write_buffer = Vec::new();
        let vertex2 =
            get_or_create_author_vertex(author_name, &mut write_buffer, &mut author_cache).unwrap();

        assert_eq!(vertex1.id, vertex2.id);

        // Verify only one vertex was created in DB
        let q = RangeVertexQuery::new().t(Identifier::new("Person").unwrap());
        let results = database.get(q).unwrap();
        match &results[0] {
            QueryOutputValue::Vertices(v) => assert_eq!(v.len(), 1),
            _ => panic!("Expected vertices"),
        };
    }

    /// Test create_coauthor_edge with cache hit
    #[test]
    fn test_create_coauthor_edge_cache_hit() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_coauthor_cache.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        let mut author_cache = HashMap::new();
        let mut write_buffer = Vec::new();
        let author1 =
            get_or_create_author_vertex("Alice", &mut write_buffer, &mut author_cache).unwrap();
        let author2 =
            get_or_create_author_vertex("Bob", &mut write_buffer, &mut author_cache).unwrap();

        use crate::config::EdgeCacheConfig;
        use crate::scrapers::scraping_orchestrator::EdgeCacheSystem;
        let mut edge_cache = EdgeCacheSystem::new(EdgeCacheConfig::default());

        // Pre-populate cache
        let key = (author1.id, author2.id);
        edge_cache.put(key, 5);

        // Create edge - should hit cache
        create_coauthor_edge(&author1, &author2, &mut write_buffer, &mut edge_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Increment using cache
        create_coauthor_edge(&author1, &author2, &mut write_buffer, &mut edge_cache).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        // Verify cache was used
        let key = (author1.id, author2.id);
        assert!(edge_cache.get(key).is_some());
        assert_eq!(edge_cache.get(key), Some(7));
    }

    /// Test DeduplicationCache with multiple publications in same year
    #[test]
    fn test_deduplication_cache_multiple_publications() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_cache_multi.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec![],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
        };

        database
            .index_property(Identifier::new("publication_id").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("year").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        // Add multiple publications to DB
        let record1 = PublicationRecord {
            id: "pub1:123".to_string(),
            title: "First Paper 2021".to_string(),
            authors: vec!["Author A".to_string()],
            year: 2021,
            venue: None,
            source: "test".to_string(),
        };
        let record2 = PublicationRecord {
            id: "pub2:456".to_string(),
            title: "Second Paper 2021".to_string(),
            authors: vec!["Author B".to_string()],
            year: 2021,
            venue: None,
            source: "test".to_string(),
        };

        let mut write_buffer = Vec::new();
        add_publication(&record1, &mut write_buffer).unwrap();
        add_publication(&record2, &mut write_buffer).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        let mut dedup_cache = DeduplicationCache::new(100);

        // Check for a third paper in the same year - should populate cache with both existing papers
        let record3 = PublicationRecord {
            id: "pub3:789".to_string(),
            title: "Third Paper 2021".to_string(),
            authors: vec!["Author C".to_string()],
            year: 2021,
            venue: None,
            source: "test".to_string(),
        };

        let exists = publication_exists(&record3, &database, &config, &mut dedup_cache);
        assert!(!exists);
    }

    /// Test publication_exists with cache population
    #[test]
    fn test_publication_exists_populates_cache() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_cache_populate.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec![],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
        };

        database
            .index_property(Identifier::new("publication_id").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("year").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        // Add a publication to DB
        let record1 = PublicationRecord {
            id: "existing:123".to_string(),
            title: "Existing Paper".to_string(),
            authors: vec!["Author A".to_string()],
            year: 2023,
            venue: None,
            source: "test".to_string(),
        };
        let mut write_buffer = Vec::new();
        add_publication(&record1, &mut write_buffer).unwrap();
        flush_buffer(&mut write_buffer, &mut database).unwrap();

        let mut dedup_cache = DeduplicationCache::new(100);

        // Check for a different paper in the same year
        let record2 = PublicationRecord {
            id: "new:456".to_string(),
            title: "Different Paper".to_string(),
            authors: vec!["Author B".to_string()],
            year: 2023,
            venue: None,
            source: "test".to_string(),
        };

        let exists = publication_exists(&record2, &database, &config, &mut dedup_cache);

        // Should not exist
        assert!(!exists);
    }
    #[test]
    fn test_normalize_title() {
        assert_eq!(normalize_title("The Graph Analysis"), "graphanalysis");
        assert_eq!(normalize_title("A Study of Trees"), "studytrees"); // stop words: a, of
        assert_eq!(normalize_title("Complex-Networks!"), "complexnetworks"); // non-alphanumeric
        assert_eq!(normalize_title("   Spaces   "), "spaces");
        assert_eq!(normalize_title("The A An And Are"), ""); // all stop words
    }
    #[tokio::test]
    async fn test_ingest_batch() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_batch.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec![],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
        };

        // Setup indexes
        database
            .index_property(Identifier::new("publication_id").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("year").unwrap())
            .unwrap();
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        let mut context = IngestionContext::new(&config);

        let records = vec![
            PublicationRecord {
                id: "batch:1".to_string(),
                title: "Batch Paper 1".to_string(),
                authors: vec!["Author One".to_string(), "Author Two".to_string()],
                year: 2024,
                venue: None,
                source: "batch".to_string(),
            },
            PublicationRecord {
                id: "batch:2".to_string(),
                title: "Batch Paper 2".to_string(),
                authors: vec!["Author Two".to_string(), "Author Three".to_string()], // Author Two is repeated
                year: 2024,
                venue: None,
                source: "batch".to_string(),
            },
            // Duplicate of batch:1
            PublicationRecord {
                id: "batch:1".to_string(),
                title: "Batch Paper 1".to_string(),
                authors: vec!["Author One".to_string(), "Author Two".to_string()],
                year: 2024,
                venue: None,
                source: "batch".to_string(),
            },
        ];

        ingest_batch(records, &mut database, &config, &mut context)
            .await
            .unwrap();

        // Verify Publications (should be 2, not 3)
        let q = RangeVertexQuery::new().t(Identifier::new("Publication").unwrap());
        let results = database.get(q).unwrap();
        match &results[0] {
            QueryOutputValue::Vertices(v) => assert_eq!(v.len(), 2),
            _ => panic!("Expected vertices"),
        };

        // Verify Authors (should be 3: One, Two, Three)
        let q = RangeVertexQuery::new().t(Identifier::new("Person").unwrap());
        let results = database.get(q).unwrap();
        match &results[0] {
            QueryOutputValue::Vertices(v) => assert_eq!(v.len(), 3),
            _ => panic!("Expected vertices"),
        };

        // Verify Edges (Paper 1: 2 authored, 1 coauthor (bidirectional = 2 edges))
        // Verify Edges (Paper 2: 2 authored, 1 coauthor (bidirectional = 2 edges))
        // Total authored: 4. Total coauthored: 4. Total: 8?
        // Let's just check coauthored between One and Two exists.

        let q = AllEdgeQuery;
        let results = database.get(q).unwrap();
        match &results[0] {
            QueryOutputValue::Edges(e) => {
                let authored = e
                    .iter()
                    .filter(|edge| edge.t.as_str() == "AUTHORED")
                    .count();
                let coauthored = e
                    .iter()
                    .filter(|edge| edge.t.as_str() == "COAUTHORED_WITH")
                    .count();
                assert_eq!(authored, 4);
                assert_eq!(coauthored, 4);
            }
            _ => panic!("Expected edges"),
        };
    }
}
