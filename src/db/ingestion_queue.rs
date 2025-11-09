//! Thread-safe publication queue with producer tracking.
//!
//! Provides a bounded buffer for PublicationRecords with backpressure handling.
//! Tracks active producers via RAII handles to detect when all have finished.

use crate::db::ingestion::PublicationRecord;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

/// Configuration for queue limits.
#[derive(Clone, Debug)]
pub struct QueueConfig {
    /// Maximum buffered publications before applying backpressure.
    pub max_queue_size: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 10000,
        }
    }
}

/// Shared state protected by mutex for thread-safe access.
struct QueueState {
    queue: VecDeque<PublicationRecord>,
    producers_done: bool,
    active_producers: usize,
}

/// Thread-safe queue for coordinating publication ingestion.
///
/// Cloneable to share across threads - each clone references the same underlying queue.
pub struct IngestionQueue {
    state: Arc<Mutex<QueueState>>,
    not_full: Arc<Condvar>,  // Notifies when queue has space
    not_empty: Arc<Condvar>, // Notifies when queue has items
    config: QueueConfig,
}

impl IngestionQueue {
    /// Creates a new queue with the specified configuration.
    pub fn new(config: QueueConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(QueueState {
                queue: VecDeque::new(),
                producers_done: false,
                active_producers: 0,
            })),
            not_full: Arc::new(Condvar::new()),
            not_empty: Arc::new(Condvar::new()),
            config,
        }
    }

    /// Increments the active producer count.
    pub fn register_producer(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| {
            eprintln!("Mutex poisoned in register_producer, recovering");
            poisoned.into_inner()
        });
        state.active_producers += 1;
    }

    /// Decrements the active producer count. Sets producers_done flag when count reaches zero.
    pub fn unregister_producer(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| {
            eprintln!("Mutex poisoned in unregister_producer, recovering");
            poisoned.into_inner()
        });
        state.active_producers = state.active_producers.saturating_sub(1);
        if state.active_producers == 0 {
            state.producers_done = true;
        }
    }

    /// Adds a publication to the queue. Blocks efficiently if queue is full.
    ///
    /// Uses condition variable to wait for space instead of polling.
    pub fn enqueue(
        &self,
        record: PublicationRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| {
            eprintln!("Mutex poisoned in enqueue, recovering");
            poisoned.into_inner()
        });

        // Wait until there's space in the queue
        while state.queue.len() >= self.config.max_queue_size {
            state = self.not_full.wait(state).unwrap_or_else(|poisoned| {
                eprintln!("Mutex poisoned while waiting on not_full condvar, recovering");
                poisoned.into_inner()
            });
        }

        state.queue.push_back(record);
        // Notify a waiting consumer that an item is available
        self.not_empty.notify_one();
        Ok(())
    }

    /// Removes and returns the next publication from the queue, or None if empty.
    ///
    /// Notifies waiting producers when space becomes available.
    pub fn dequeue(&self) -> Option<PublicationRecord> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| {
            eprintln!("Mutex poisoned in dequeue, recovering");
            poisoned.into_inner()
        });
        let result = state.queue.pop_front();
        if result.is_some() {
            // Notify a waiting producer that space is available
            self.not_full.notify_one();
        }
        result
    }

    /// Returns the current number of publications in the queue.
    pub fn queue_size(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| {
                eprintln!("Mutex poisoned in queue_size, recovering");
                poisoned.into_inner()
            })
            .queue
            .len()
    }

    /// Returns true if all producers have finished.  
    ///  
    /// Producers are considered finished when all registered producers have unregistered,  
    /// and the internal `producers_done` flag is set.
    pub fn producers_finished(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| {
                eprintln!("Mutex poisoned in producers_finished, recovering");
                poisoned.into_inner()
            })
            .producers_done
    }

    /// Returns the number of currently active producers.
    pub fn active_producer_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| {
                eprintln!("Mutex poisoned in active_producer_count, recovering");
                poisoned.into_inner()
            })
            .active_producers
    }

    /// Creates a RAII producer handle that auto-registers/unregisters.
    pub fn create_producer(&self) -> QueueProducer {
        self.register_producer();
        QueueProducer {
            queue: self.clone(),
        }
    }
}

impl Clone for IngestionQueue {
    /// Clones the Arc reference to share the same underlying queue across threads.
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            not_full: Arc::clone(&self.not_full),
            not_empty: Arc::clone(&self.not_empty),
            config: self.config.clone(),
        }
    }
}

/// RAII handle that auto-unregisters producer on drop, even if scraper panics.
pub struct QueueProducer {
    queue: IngestionQueue,
}

impl QueueProducer {
    /// Adds a publication to the queue. Delegates to the queue's enqueue method.
    pub fn submit(
        &self,
        record: PublicationRecord,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.queue.enqueue(record)
    }
}

impl Drop for QueueProducer {
    /// Automatically unregisters producer. Sets producers_done when last producer drops.
    fn drop(&mut self) {
        self.queue.unregister_producer();
    }
}
