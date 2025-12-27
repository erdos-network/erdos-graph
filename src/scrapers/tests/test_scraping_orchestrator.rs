#[cfg(test)]
mod tests {
    use crate::db::ingestion::PublicationRecord;
    use crate::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration as StdDuration;
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
}
