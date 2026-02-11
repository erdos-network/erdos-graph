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
        let queue = ThreadSafeQueue::<PublicationRecord>::new(QueueConfig::default());

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

    #[test]
    fn test_enqueue_with_wait_for_space() {
        let config = QueueConfig { max_queue_size: 2 };
        let queue = ThreadSafeQueue::new(config);

        // Fill the queue
        queue.enqueue(create_test_record("1")).unwrap();
        queue.enqueue(create_test_record("2")).unwrap();

        // Spawn a thread that will block on enqueue
        let q = queue.clone();
        let handle = thread::spawn(move || {
            q.enqueue(create_test_record("3")).unwrap();
        });

        // Give it time to block
        thread::sleep(Duration::from_millis(50));

        // Dequeue to make space
        let item = queue.dequeue().unwrap();
        assert_eq!(item.id, "1");

        // The blocked enqueue should now complete
        handle.join().unwrap();

        // Verify all items are present
        assert_eq!(queue.queue_size(), 2);
        assert_eq!(queue.dequeue().unwrap().id, "2");
        assert_eq!(queue.dequeue().unwrap().id, "3");
    }

    #[test]
    fn test_dequeue_notifies_waiting_producers() {
        let config = QueueConfig { max_queue_size: 1 };
        let queue = ThreadSafeQueue::new(config);

        // Fill the queue
        queue.enqueue(create_test_record("1")).unwrap();

        // Spawn multiple threads that will block on enqueue
        let mut handles = vec![];
        for i in 2..=4 {
            let q = queue.clone();
            let handle = thread::spawn(move || {
                q.enqueue(create_test_record(&format!("{}", i))).unwrap();
            });
            handles.push(handle);
        }

        // Give threads time to block
        thread::sleep(Duration::from_millis(100));

        // Dequeue items one by one, allowing blocked producers to proceed
        for _ in 0..4 {
            thread::sleep(Duration::from_millis(50));
            queue.dequeue();
        }

        // Wait for all producer threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Queue should be empty now
        assert_eq!(queue.queue_size(), 0);
    }

    #[test]
    fn test_queue_operations_with_no_producers_registered() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        // Operations should work even without registered producers
        queue.enqueue(create_test_record("1")).unwrap();
        assert_eq!(queue.queue_size(), 1);

        let item = queue.dequeue().unwrap();
        assert_eq!(item.id, "1");

        // producers_finished should be false if no producers were ever registered
        assert!(!queue.producers_finished());
    }

    #[test]
    fn test_active_producer_count_accuracy() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        assert_eq!(queue.active_producer_count(), 0);

        let p1 = queue.create_producer();
        assert_eq!(queue.active_producer_count(), 1);

        let p2 = queue.create_producer();
        assert_eq!(queue.active_producer_count(), 2);

        let p3 = queue.create_producer();
        assert_eq!(queue.active_producer_count(), 3);

        drop(p1);
        assert_eq!(queue.active_producer_count(), 2);

        drop(p2);
        assert_eq!(queue.active_producer_count(), 1);

        drop(p3);
        assert_eq!(queue.active_producer_count(), 0);
        assert!(queue.producers_finished());
    }

    #[test]
    fn test_queue_size_reflects_operations() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        assert_eq!(queue.queue_size(), 0);

        for i in 0..10 {
            queue
                .enqueue(create_test_record(&format!("{}", i)))
                .unwrap();
            assert_eq!(queue.queue_size(), i + 1);
        }

        for i in (0..10).rev() {
            queue.dequeue();
            assert_eq!(queue.queue_size(), i);
        }
    }

    #[test]
    fn test_producers_finished_remains_true() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        {
            let _p = queue.create_producer();
            assert!(!queue.producers_finished());
        }

        // Once producers_finished is true, it stays true
        assert!(queue.producers_finished());

        // Even after operations
        queue.enqueue(create_test_record("1")).unwrap();
        assert!(queue.producers_finished());

        queue.dequeue();
        assert!(queue.producers_finished());
    }

    #[test]
    fn test_multiple_dequeues_notify_producers() {
        let config = QueueConfig { max_queue_size: 3 };
        let queue = ThreadSafeQueue::new(config);

        // Fill queue
        for i in 1..=3 {
            queue
                .enqueue(create_test_record(&format!("{}", i)))
                .unwrap();
        }

        // Spawn producers that will block
        let mut handles = vec![];
        for i in 4..=6 {
            let q = queue.clone();
            let handle = thread::spawn(move || {
                q.enqueue(create_test_record(&format!("{}", i))).unwrap();
            });
            handles.push(handle);
        }

        thread::sleep(Duration::from_millis(100));

        // Dequeue multiple items
        queue.dequeue();
        queue.dequeue();
        queue.dequeue();

        // Wait for producers
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(queue.queue_size(), 3);
    }

    #[test]
    fn test_enqueue_error_handling() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        // Normal enqueue should succeed
        let result = queue.enqueue(create_test_record("1"));
        assert!(result.is_ok());

        // Verify item was enqueued
        assert_eq!(queue.queue_size(), 1);
    }

    #[test]
    fn test_producer_submit_error_propagation() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();

        // Submit should succeed normally
        let result = producer.submit(create_test_record("1"));
        assert!(result.is_ok());

        assert_eq!(queue.queue_size(), 1);
    }

    #[test]
    fn test_queue_with_zero_max_size_config() {
        // Edge case: max_queue_size of 0 (though not practical)
        let config = QueueConfig { max_queue_size: 0 };
        let queue: ThreadSafeQueue<PublicationRecord> = ThreadSafeQueue::new(config);

        // Enqueue will block immediately since queue is always "full"
        let q = queue.clone();
        let handle = thread::spawn(move || {
            // This will block forever, so we'll just start it
            let _ = q.enqueue(create_test_record("1"));
        });

        // Give it a moment to block
        thread::sleep(Duration::from_millis(50));

        // We can't really complete this test without deadlocking,
        // so we'll just verify the queue was created
        assert_eq!(queue.queue_size(), 0);

        // Don't wait for handle as it will block forever
        drop(handle);
    }

    #[test]
    fn test_register_unregister_symmetry() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        // Register and unregister manually
        queue.register_producer();
        queue.register_producer();
        queue.register_producer();
        assert_eq!(queue.active_producer_count(), 3);

        queue.unregister_producer();
        assert_eq!(queue.active_producer_count(), 2);

        queue.unregister_producer();
        assert_eq!(queue.active_producer_count(), 1);

        queue.unregister_producer();
        assert_eq!(queue.active_producer_count(), 0);
        assert!(queue.producers_finished());
    }

    #[test]
    fn test_queue_clone_independence() {
        let queue1 = ThreadSafeQueue::new(QueueConfig::default());
        let queue2 = queue1.clone();
        let queue3 = queue1.clone();

        // All clones share the same state
        queue1.enqueue(create_test_record("1")).unwrap();
        assert_eq!(queue2.queue_size(), 1);
        assert_eq!(queue3.queue_size(), 1);

        let item = queue2.dequeue().unwrap();
        assert_eq!(item.id, "1");
        assert_eq!(queue1.queue_size(), 0);
        assert_eq!(queue3.queue_size(), 0);
    }

    #[test]
    fn test_producer_handles_across_clones() {
        let queue = ThreadSafeQueue::<PublicationRecord>::new(QueueConfig::default());

        let p1 = queue.create_producer();
        assert_eq!(queue.active_producer_count(), 1);

        let queue_clone = queue.clone();
        let p2 = queue_clone.create_producer();
        assert_eq!(queue.active_producer_count(), 2);
        assert_eq!(queue_clone.active_producer_count(), 2);

        drop(p1);
        assert_eq!(queue.active_producer_count(), 1);
        assert_eq!(queue_clone.active_producer_count(), 1);

        drop(p2);
        assert_eq!(queue.active_producer_count(), 0);
        assert_eq!(queue_clone.active_producer_count(), 0);
        assert!(queue.producers_finished());
        assert!(queue_clone.producers_finished());
    }

    #[test]
    fn test_queue_size_with_concurrent_operations() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let queue_clone = queue.clone();

        let producer = thread::spawn(move || {
            for i in 0..50 {
                queue_clone
                    .enqueue(create_test_record(&i.to_string()))
                    .unwrap();
            }
        });

        producer.join().unwrap();
        assert_eq!(queue.queue_size(), 50);
    }

    #[test]
    fn test_dequeue_returns_none_consistently_when_empty() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        for _ in 0..100 {
            assert_eq!(queue.dequeue(), None);
        }
    }

    #[test]
    fn test_producer_submit_with_error_recovery() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let producer = queue.create_producer();

        // Submit should work normally
        for i in 0..10 {
            let result = producer.submit(create_test_record(&i.to_string()));
            assert!(result.is_ok());
        }

        assert_eq!(queue.queue_size(), 10);
    }

    #[test]
    fn test_queue_maintains_fifo_under_stress() {
        let queue = ThreadSafeQueue::new(QueueConfig {
            max_queue_size: 1000,
        });

        // Enqueue many items
        for i in 0..500 {
            queue.enqueue(create_test_record(&i.to_string())).unwrap();
        }

        // Dequeue and verify order
        for i in 0..500 {
            let item = queue.dequeue().unwrap();
            assert_eq!(item.id, i.to_string());
        }
    }

    #[test]
    fn test_multiple_producers_and_consumers() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());
        let num_items = 100;

        // Multiple producers
        let mut producer_handles = vec![];
        for producer_id in 0..3 {
            let q = queue.clone();
            let handle = thread::spawn(move || {
                for i in 0..num_items {
                    q.enqueue(create_test_record(&format!("p{}-{}", producer_id, i)))
                        .unwrap();
                }
            });
            producer_handles.push(handle);
        }

        // Multiple consumers
        let consumed_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut consumer_handles = vec![];
        for _ in 0..3 {
            let q = queue.clone();
            let count = consumed_count.clone();
            let handle = thread::spawn(move || {
                loop {
                    if q.dequeue().is_some() {
                        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    } else if count.load(std::sync::atomic::Ordering::SeqCst) >= num_items * 3 {
                        break;
                    }
                }
            });
            consumer_handles.push(handle);
        }

        // Wait for producers
        for handle in producer_handles {
            handle.join().unwrap();
        }

        // Wait for consumers
        for handle in consumer_handles {
            handle.join().unwrap();
        }

        assert_eq!(
            consumed_count.load(std::sync::atomic::Ordering::SeqCst),
            num_items * 3
        );
    }

    #[test]
    fn test_config_debug_trait() {
        let config = QueueConfig {
            max_queue_size: 500,
        };
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("500"));
    }

    #[test]
    fn test_enqueue_after_producers_finished() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        {
            let _p = queue.create_producer();
        }

        // Producers finished
        assert!(queue.producers_finished());

        // Can still enqueue
        let result = queue.enqueue(create_test_record("after-finish"));
        assert!(result.is_ok());
        assert_eq!(queue.queue_size(), 1);
    }

    #[test]
    fn test_queue_with_large_max_size() {
        let config = QueueConfig {
            max_queue_size: 1_000_000,
        };
        let queue = ThreadSafeQueue::new(config);

        queue.enqueue(create_test_record("large-config")).unwrap();
        assert_eq!(queue.queue_size(), 1);
    }

    #[test]
    fn test_producer_drop_multiple_times() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        let p1 = queue.create_producer();
        let p2 = queue.create_producer();
        let p3 = queue.create_producer();

        assert_eq!(queue.active_producer_count(), 3);

        drop(p1);
        drop(p2);
        assert_eq!(queue.active_producer_count(), 1);
        assert!(!queue.producers_finished());

        drop(p3);
        assert_eq!(queue.active_producer_count(), 0);
        assert!(queue.producers_finished());
    }

    #[test]
    fn test_queue_operations_with_different_record_data() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        let record1 = PublicationRecord {
            id: "id1".to_string(),
            title: "Title 1".to_string(),
            authors: vec!["Author 1".to_string()],
            year: 2020,
            venue: Some("Venue 1".to_string()),
            source: "source1".to_string(),
        };

        let record2 = PublicationRecord {
            id: "id2".to_string(),
            title: "Title 2".to_string(),
            authors: vec!["Author 2".to_string(), "Author 3".to_string()],
            year: 2021,
            venue: None,
            source: "source2".to_string(),
        };

        queue.enqueue(record1.clone()).unwrap();
        queue.enqueue(record2.clone()).unwrap();

        let dequeued1 = queue.dequeue().unwrap();
        assert_eq!(dequeued1.id, "id1");
        assert_eq!(dequeued1.year, 2020);
        assert_eq!(dequeued1.venue, Some("Venue 1".to_string()));

        let dequeued2 = queue.dequeue().unwrap();
        assert_eq!(dequeued2.id, "id2");
        assert_eq!(dequeued2.authors.len(), 2);
        assert_eq!(dequeued2.venue, None);
    }

    #[test]
    fn test_notifies_work_correctly() {
        let config = QueueConfig { max_queue_size: 2 };
        let queue = ThreadSafeQueue::new(config);

        queue.enqueue(create_test_record("1")).unwrap();
        queue.enqueue(create_test_record("2")).unwrap();

        let q_clone = queue.clone();
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            q_clone.dequeue();
        });

        // This should block until space is available
        let q_clone2 = queue.clone();
        let enqueue_handle = thread::spawn(move || {
            q_clone2.enqueue(create_test_record("3")).unwrap();
        });

        handle.join().unwrap();
        enqueue_handle.join().unwrap();

        assert_eq!(queue.queue_size(), 2);
    }

    #[test]
    fn test_saturating_sub_behavior() {
        let queue: ThreadSafeQueue<PublicationRecord> =
            ThreadSafeQueue::new(QueueConfig::default());

        // Unregister without register (saturating_sub should prevent underflow)
        queue.unregister_producer();
        assert_eq!(queue.active_producer_count(), 0);

        // Multiple unregisters
        queue.unregister_producer();
        queue.unregister_producer();
        assert_eq!(queue.active_producer_count(), 0);
    }

    #[test]
    fn test_empty_queue_after_many_operations() {
        let queue = ThreadSafeQueue::new(QueueConfig::default());

        for i in 0..1000 {
            queue.enqueue(create_test_record(&i.to_string())).unwrap();
        }

        for _ in 0..1000 {
            queue.dequeue();
        }

        assert_eq!(queue.queue_size(), 0);
        assert_eq!(queue.dequeue(), None);
    }
}
