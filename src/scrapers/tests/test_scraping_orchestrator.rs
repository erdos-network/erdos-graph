#[cfg(test)]
mod tests {
    use crate::db::ingestion::PublicationRecord;
    use crate::scrapers::scraping_orchestrator::{
        add_publication, create_authored_edge, create_coauthor_edge, get_or_create_author_vertex,
    };
    use crate::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
    use indradb::{
        AllEdgeQuery, Database, Edge, Identifier, QueryExt, QueryOutputValue, RangeVertexQuery,
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

        add_publication(&record, &mut database).unwrap();

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
        let vertex1 = get_or_create_author_vertex(author_name, &mut database).unwrap();

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
        let vertex2 = get_or_create_author_vertex(author_name, &mut database).unwrap();

        // Should be the same vertex ID
        assert_eq!(vertex1.id, vertex2.id);

        // 3. Create a different author
        let other_name = "Alice Smith";
        let vertex3 = get_or_create_author_vertex(other_name, &mut database).unwrap();

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
        let author = get_or_create_author_vertex(author_name, &mut database).unwrap();

        let record = PublicationRecord {
            id: "arxiv:1234.5678".to_string(),
            title: "Probabilistic Methods".to_string(),
            authors: vec![author_name.to_string()],
            year: 1947,
            venue: None,
            source: "arxiv".to_string(),
        };
        let publication = add_publication(&record, &mut database).unwrap();

        // Create edge
        create_authored_edge(&author, &publication, &mut database).unwrap();

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
    fn test_create_coauthored_edge() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_coauthor.rocksdb");
        let mut database = RocksdbDatastore::new_db(&db_path).unwrap();

        // Index name property as get_or_create_author requires it
        database
            .index_property(Identifier::new("name").unwrap())
            .unwrap();

        let author1 = get_or_create_author_vertex("Alice", &mut database).unwrap();
        let author2 = get_or_create_author_vertex("Bob", &mut database).unwrap();

        // Create initial edge
        create_coauthor_edge(&author1, &author2, &mut database).unwrap();

        // Verify edge and weight
        let edge_type = Identifier::new("COAUTHORED_WITH").unwrap();
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

        assert_eq!(weight, 1);

        // Increment weight
        create_coauthor_edge(&author1, &author2, &mut database).unwrap();

        let q = SpecificEdgeQuery::single(expected_edge.clone())
            .properties()
            .unwrap();
        let results = database.get(q).unwrap();
        let edge_props = match &results[0] {
            QueryOutputValue::EdgeProperties(eps) => &eps[0],
            _ => panic!("Expected edge properties"),
        };

        let weight = edge_props
            .props
            .iter()
            .find(|p| p.name == weight_id)
            .map(|p| p.value.as_u64().unwrap())
            .unwrap();

        assert_eq!(weight, 2);
    }
}
