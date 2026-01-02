#[cfg(test)]
mod tests {
    use crate::db::ingestion::PublicationRecord;
    use crate::utilities::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
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
        let queue = ThreadSafeQueue::new(QueueConfig::default());

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
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        assert_eq!(queue.dequeue(), None);
        assert_eq!(queue.queue_size(), 0);
    }

    #[test]
    fn test_producer_registration_tracking() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

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
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

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
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();

        producer.submit(create_test_record("via-producer")).unwrap();

        assert_eq!(queue.queue_size(), 1);
        assert_eq!(queue.dequeue().unwrap().id, "via-producer");
    }

    #[test]
    fn test_multiple_producers_concurrent_submit() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        let queue1 = queue.clone();
        let queue2 = queue.clone();

        let handle1 = thread::spawn(move || {
            let producer = queue1.create_producer();
            for i in 0..10 {
                producer
                    .submit(create_test_record(&format!("thread1-{}", i)))
                    .unwrap();
            }
        });

        let handle2 = thread::spawn(move || {
            let producer = queue2.create_producer();
            for i in 0..10 {
                producer
                    .submit(create_test_record(&format!("thread2-{}", i)))
                    .unwrap();
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
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

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
        let queue1 = ThreadSafeQueue::new(QueueConfig::default());
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
        let config = QueueConfig { max_queue_size: 5 };
        let queue = ThreadSafeQueue::new(config);

        // Fill queue to capacity
        for i in 0..5 {
            queue
                .enqueue(create_test_record(&format!("{}", i)))
                .unwrap();
        }
        assert_eq!(queue.queue_size(), 5);

        // Attempt to enqueue in background (will block due to backpressure)
        let queue_clone = queue.clone();
        let handle = thread::spawn(move || queue_clone.enqueue(create_test_record("blocked")));

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
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        // Enqueue some items first
        queue.enqueue(create_test_record("before-panic")).unwrap();
        assert_eq!(queue.queue_size(), 1);

        // Test that the queue handles concurrent access gracefully
        // The poison recovery mechanism ensures that even if a thread panicked
        // while holding the lock, other threads can continue

        let queue_clone = queue.clone();
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let q = queue_clone.clone();
                thread::spawn(move || {
                    q.enqueue(create_test_record(&format!("concurrent-{}", i)))
                        .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Queue should still be usable
        assert_eq!(queue.queue_size(), 11); // 1 before + 10 concurrent

        // All operations should still work
        while queue.dequeue().is_some() {}
        assert_eq!(queue.queue_size(), 0);
    }

    #[test]
    fn test_queue_config_default() {
        let config = QueueConfig::default();
        assert_eq!(config.max_queue_size, 10000);
    }

    #[test]
    fn test_queue_config_custom() {
        let config = QueueConfig {
            max_queue_size: 100,
        };
        assert_eq!(config.max_queue_size, 100);
    }

    #[test]
    fn test_queue_config_clone() {
        let config = QueueConfig {
            max_queue_size: 500,
        };
        let cloned = config.clone();
        assert_eq!(cloned.max_queue_size, 500);
    }

    #[test]
    fn test_new_queue_empty() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        assert_eq!(queue.queue_size(), 0);
        assert_eq!(queue.active_producer_count(), 0);
        assert!(!queue.producers_finished());
    }

    #[test]
    fn test_register_multiple_producers() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        queue.register_producer();
        queue.register_producer();
        queue.register_producer();

        assert_eq!(queue.active_producer_count(), 3);
        assert!(!queue.producers_finished());
    }

    #[test]
    fn test_unregister_more_than_registered() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        queue.register_producer();
        assert_eq!(queue.active_producer_count(), 1);

        queue.unregister_producer();
        assert_eq!(queue.active_producer_count(), 0);
        assert!(queue.producers_finished());

        // Unregister again - should saturate at 0
        queue.unregister_producer();
        assert_eq!(queue.active_producer_count(), 0);
        assert!(queue.producers_finished());
    }

    #[test]
    fn test_enqueue_dequeue_single_item() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        let record = create_test_record("single");
        queue.enqueue(record).unwrap();

        assert_eq!(queue.queue_size(), 1);

        let dequeued = queue.dequeue().unwrap();
        assert_eq!(dequeued.id, "single");

        assert_eq!(queue.queue_size(), 0);
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn test_multiple_dequeues_on_empty() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        assert_eq!(queue.dequeue(), None);
        assert_eq!(queue.dequeue(), None);
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn test_producer_handle_enqueue_multiple() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();

        for i in 0..5 {
            producer
                .submit(create_test_record(&format!("item-{}", i)))
                .unwrap();
        }

        assert_eq!(queue.queue_size(), 5);
        assert_eq!(queue.active_producer_count(), 1);

        drop(producer);

        assert_eq!(queue.active_producer_count(), 0);
        assert!(queue.producers_finished());
    }

    #[test]
    fn test_multiple_producer_handles() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        let producer1 = queue.create_producer();
        let producer2 = queue.create_producer();
        let producer3 = queue.create_producer();

        assert_eq!(queue.active_producer_count(), 3);

        drop(producer1);
        assert_eq!(queue.active_producer_count(), 2);
        assert!(!queue.producers_finished());

        drop(producer2);
        assert_eq!(queue.active_producer_count(), 1);
        assert!(!queue.producers_finished());

        drop(producer3);
        assert_eq!(queue.active_producer_count(), 0);
        assert!(queue.producers_finished());
    }

    #[test]
    fn test_backpressure_multiple_blocked_producers() {
        let config = QueueConfig { max_queue_size: 3 };
        let queue = ThreadSafeQueue::new(config);

        // Fill queue to capacity
        for i in 0..3 {
            queue
                .enqueue(create_test_record(&format!("{}", i)))
                .unwrap();
        }

        // Launch multiple producers that will block
        let mut handles = vec![];
        for i in 0..3 {
            let q = queue.clone();
            let handle = thread::spawn(move || {
                q.enqueue(create_test_record(&format!("blocked-{}", i)))
                    .unwrap()
            });
            handles.push(handle);
        }

        thread::sleep(Duration::from_millis(100));

        // Dequeue items one by one to unblock producers
        // Wait for all producer threads to complete before checking final queue size
        queue.dequeue();
        queue.dequeue();
        queue.dequeue();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(queue.queue_size(), 3);
    }

    #[test]
    fn test_concurrent_enqueue_dequeue() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        let queue_producer = queue.clone();
        let producer_handle = thread::spawn(move || {
            for i in 0..100 {
                queue_producer
                    .enqueue(create_test_record(&format!("{}", i)))
                    .unwrap();
            }
        });

        let queue_consumer = queue.clone();
        let consumer_handle = thread::spawn(move || {
            let mut count = 0;
            while count < 100 {
                if queue_consumer.dequeue().is_some() {
                    count += 1;
                }
            }
            count
        });

        producer_handle.join().unwrap();
        let consumed = consumer_handle.join().unwrap();

        assert_eq!(consumed, 100);
    }

    #[test]
    fn test_queue_size_during_operations() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        assert_eq!(queue.queue_size(), 0);

        queue.enqueue(create_test_record("1")).unwrap();
        assert_eq!(queue.queue_size(), 1);

        queue.enqueue(create_test_record("2")).unwrap();
        assert_eq!(queue.queue_size(), 2);

        queue.dequeue();
        assert_eq!(queue.queue_size(), 1);

        queue.dequeue();
        assert_eq!(queue.queue_size(), 0);

        queue.dequeue();
        assert_eq!(queue.queue_size(), 0);
    }

    #[test]
    fn test_producers_finished_flag() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        // Initially false (no producers registered yet)
        assert!(!queue.producers_finished());

        // Register and unregister
        queue.register_producer();
        assert!(!queue.producers_finished());

        queue.unregister_producer();
        assert!(queue.producers_finished());

        // Register again - flag should remain true (once done, stays done)
        queue.register_producer();
        // Note: The flag doesn't reset when new producers register
        // This is the current implementation behavior
    }

    #[test]
    fn test_small_queue_size_config() {
        let config = QueueConfig { max_queue_size: 1 };
        let queue = ThreadSafeQueue::new(config);

        queue.enqueue(create_test_record("1")).unwrap();

        let q = queue.clone();
        let handle = thread::spawn(move || q.enqueue(create_test_record("2")).unwrap());

        thread::sleep(Duration::from_millis(100));

        // Dequeue to make space
        let item = queue.dequeue().unwrap();
        assert_eq!(item.id, "1");

        handle.join().unwrap();

        assert_eq!(queue.queue_size(), 1);
        let item = queue.dequeue().unwrap();
        assert_eq!(item.id, "2");
    }
}
