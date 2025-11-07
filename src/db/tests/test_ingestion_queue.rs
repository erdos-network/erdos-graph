use crate::db::ingestion::PublicationRecord;
use crate::db::ingestion_queue::{IngestionQueue, QueueConfig};
use std::thread;
use std::time::{Duration, Instant};

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
fn test_should_stop_when_producers_done_and_queue_empty() {
    let queue = IngestionQueue::new(QueueConfig::default());
    let start_time = Instant::now();
    
    // Initially should not stop
    assert!(queue.should_stop(start_time).is_none());
    
    // Register and unregister producer to set producers_done
    let producer = queue.create_producer();
    drop(producer);
    
    // Should stop when producers done and queue empty
    let reason = queue.should_stop(start_time);
    assert!(reason.is_some());
    assert!(reason.unwrap().contains("All producers finished"));
}

#[test]
fn test_should_stop_respects_idle_timeout() {
    let config = QueueConfig {
        idle_timeout: Duration::from_millis(50),
        max_processing_time: Duration::from_secs(10),
        max_queue_size: 100,
    };
    let queue = IngestionQueue::new(config);
    let start_time = Instant::now();
    
    // Add a job to set last_job_time
    queue.enqueue(create_test_record("test")).unwrap();
    queue.dequeue();
    
    // Should not stop immediately
    assert!(queue.should_stop(start_time).is_none());
    
    // Wait for idle timeout
    thread::sleep(Duration::from_millis(60));
    
    // Should stop due to idle timeout
    let reason = queue.should_stop(start_time);
    assert!(reason.is_some());
    assert!(reason.unwrap().contains("Idle timeout"));
}

#[test]
fn test_should_stop_respects_max_processing_time() {
    let config = QueueConfig {
        idle_timeout: Duration::from_secs(10),
        max_processing_time: Duration::from_millis(50),
        max_queue_size: 100,
    };
    let queue = IngestionQueue::new(config);
    let start_time = Instant::now();
    
    // Keep adding jobs to prevent idle timeout
    queue.enqueue(create_test_record("test")).unwrap();
    
    // Should not stop immediately
    assert!(queue.should_stop(start_time).is_none());
    
    // Wait for max processing time
    thread::sleep(Duration::from_millis(60));
    
    // Should stop due to max processing time
    let reason = queue.should_stop(start_time);
    assert!(reason.is_some());
    assert!(reason.unwrap().contains("Max processing time"));
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
        idle_timeout: Duration::from_secs(10),
        max_processing_time: Duration::from_secs(10),
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
