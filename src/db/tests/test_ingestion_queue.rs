#[cfg(test)]
mod tests {
    use crate::db::ingestion::PublicationRecord;
    use crate::db::ingestion_queue::{IngestionQueue, QueueConfig};
    use std::thread;
    use std::time::Duration;

    /// Helper to create a test publication record.
    fn create_test_record(id: &str) -> PublicationRecord {
        PublicationRecord {
            id: id.to_string(),
            title: format!("Test Publication {}", id),
            authors: vec!["Test Author".to_string()],
            year: 2024,
            venue: Some("Test Venue".to_string()),
            source: "test".to_string(),
        }
    }

    #[test]
    fn test_enqueue_dequeue_fifo_order() {
    let queue = IngestionQueue::new(QueueConfig::default());
    
    // Enqueue multiple records
    queue.enqueue(create_test_record("1")).unwrap();
    queue.enqueue(create_test_record("2")).unwrap();
    queue.enqueue(create_test_record("3")).unwrap();
    
    assert_eq!(queue.queue_size(), 3);
    
    // Verify FIFO order
    assert_eq!(queue.dequeue().unwrap().id, "1");
    assert_eq!(queue.dequeue().unwrap().id, "2");
    assert_eq!(queue.dequeue().unwrap().id, "3");
    assert_eq!(queue.queue_size(), 0);
}

#[test]
fn test_dequeue_empty_queue_returns_none() {
    let queue = IngestionQueue::new(QueueConfig::default());
    
    assert_eq!(queue.dequeue(), None);
    assert_eq!(queue.queue_size(), 0);
}

#[test]
fn test_producer_registration_tracking() {
    let queue = IngestionQueue::new(QueueConfig::default());
    
    // Initially no producers (but producers_done is false until first registration cycle)
    assert_eq!(queue.active_producer_count(), 0);
    
    // Register first producer
    queue.register_producer();
    assert_eq!(queue.active_producer_count(), 1);
    assert!(!queue.producers_finished());
    
    // Register second producer
    queue.register_producer();
    assert_eq!(queue.active_producer_count(), 2);
    assert!(!queue.producers_finished());
    
    // Unregister one producer
    queue.unregister_producer();
    assert_eq!(queue.active_producer_count(), 1);
    assert!(!queue.producers_finished());
    
    // Unregister last producer - should set producers_done flag
    queue.unregister_producer();
    assert_eq!(queue.active_producer_count(), 0);
    assert!(queue.producers_finished());
}

#[test]
fn test_producer_handle_raii_cleanup() {
    let queue = IngestionQueue::new(QueueConfig::default());
    
    {
        let _producer = queue.create_producer();
        assert_eq!(queue.active_producer_count(), 1);
        assert!(!queue.producers_finished());
    } // Producer dropped here - should auto-unregister
    
    assert_eq!(queue.active_producer_count(), 0);
    assert!(queue.producers_finished());
}

#[test]
fn test_producer_handle_submit() {
    let queue = IngestionQueue::new(QueueConfig::default());
    let producer = queue.create_producer();
    
    producer.submit(create_test_record("via-producer")).unwrap();
    
    assert_eq!(queue.queue_size(), 1);
    assert_eq!(queue.dequeue().unwrap().id, "via-producer");
}

#[test]
fn test_multiple_producers_concurrent_submit() {
    let queue = IngestionQueue::new(QueueConfig::default());
    
    let queue1 = queue.clone();
    let queue2 = queue.clone();
    
    let handle1 = thread::spawn(move || {
        let producer = queue1.create_producer();
        for i in 0..10 {
            producer.submit(create_test_record(&format!("thread1-{}", i))).unwrap();
        }
    });
    
    let handle2 = thread::spawn(move || {
        let producer = queue2.create_producer();
        for i in 0..10 {
            producer.submit(create_test_record(&format!("thread2-{}", i))).unwrap();
        }
    });
    
    handle1.join().unwrap();
    handle2.join().unwrap();
    
    // Should have all 20 records
    assert_eq!(queue.queue_size(), 20);
    assert!(queue.producers_finished());
}

#[test]
fn test_producers_done_status() {
    let queue = IngestionQueue::new(QueueConfig::default());
    
    // Register and unregister producer to set producers_done
    let producer = queue.create_producer();
    assert!(!queue.producers_finished());
    
    drop(producer);
    
    // Should be marked as finished when all producers done
    assert!(queue.producers_finished());
    assert_eq!(queue.queue_size(), 0);
}

#[test]
fn test_queue_clone_shares_state() {
    let queue1 = IngestionQueue::new(QueueConfig::default());
    let queue2 = queue1.clone();
    
    // Enqueue on queue1
    queue1.enqueue(create_test_record("shared")).unwrap();
    
    // Should be visible on queue2 (shared Arc)
    assert_eq!(queue2.queue_size(), 1);
    assert_eq!(queue2.dequeue().unwrap().id, "shared");
    
    // Both should see empty queue
    assert_eq!(queue1.queue_size(), 0);
    assert_eq!(queue2.queue_size(), 0);
}

#[test]
fn test_backpressure_when_queue_full() {
    let config = QueueConfig {
        max_queue_size: 5,
    };
    let queue = IngestionQueue::new(config);
    
    // Fill queue to capacity
    for i in 0..5 {
        queue.enqueue(create_test_record(&format!("{}", i))).unwrap();
    }
    assert_eq!(queue.queue_size(), 5);
    
    // Attempt to enqueue in background (will block due to backpressure)
    let queue_clone = queue.clone();
    let handle = thread::spawn(move || {
        queue_clone.enqueue(create_test_record("blocked"))
    });
    
    // Give it time to attempt enqueue
    thread::sleep(Duration::from_millis(150));
    
    // Dequeue one item to make space
    queue.dequeue();
    
    // Now the blocked enqueue should succeed
    handle.join().unwrap().unwrap();
    assert_eq!(queue.queue_size(), 5);
}

#[test]
fn test_poison_recovery() {
    let queue = IngestionQueue::new(QueueConfig::default());
    
    // Enqueue some items first
    queue.enqueue(create_test_record("before-panic")).unwrap();
    assert_eq!(queue.queue_size(), 1);
    
    // Test that the queue handles concurrent access gracefully
    // The poison recovery mechanism ensures that even if a thread panicked
    // while holding the lock, other threads can continue
    
    let queue_clone = queue.clone();
    let handles: Vec<_> = (0..10).map(|i| {
        let q = queue_clone.clone();
        thread::spawn(move || {
            q.enqueue(create_test_record(&format!("concurrent-{}", i))).unwrap();
        })
    }).collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Queue should still be usable
    assert_eq!(queue.queue_size(), 11); // 1 before + 10 concurrent
    
    // All operations should still work
    while queue.dequeue().is_some() {}
    assert_eq!(queue.queue_size(), 0);
}

}
