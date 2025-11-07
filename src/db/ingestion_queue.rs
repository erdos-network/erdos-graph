//! Thread-safe publication queue with producer tracking and timeout control.
//!
//! Provides a bounded buffer for PublicationRecords with backpressure handling.
//! Tracks active producers via RAII handles to detect when all have finished.

use crate::db::ingestion::PublicationRecord;
use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Configuration for queue timeouts and limits.
#[derive(Clone, Debug)]
pub struct QueueConfig {
    /// Stop if no new jobs arrive within this duration.
    pub idle_timeout: Duration,
    /// Stop after this total processing time regardless of activity.
    pub max_processing_time: Duration,
    /// Maximum buffered publications before applying backpressure.
    pub max_queue_size: usize,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(30),
            max_processing_time: Duration::from_secs(3600),
            max_queue_size: 10000,
        }
    }
}

/// Shared state protected by mutex for thread-safe access.
struct QueueState {
    queue: VecDeque<PublicationRecord>,
    last_job_time: Option<Instant>,
    producers_done: bool,
    active_producers: usize,
}

/// Thread-safe queue for coordinating publication ingestion.
///
/// Cloneable to share across threads - each clone references the same underlying queue.
pub struct IngestionQueue {
    state: Arc<Mutex<QueueState>>,
    config: QueueConfig,
}

impl IngestionQueue {
    /// Creates a new queue with the given configuration.
    pub fn new(config: QueueConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(QueueState {
                queue: VecDeque::new(),
                last_job_time: None,
                producers_done: false,
                active_producers: 0,
            })),
            config,
        }
    }
    
    /// Increments the active producer count.
    pub fn register_producer(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_producers += 1;
    }
    
    /// Decrements the active producer count. Sets producers_done flag when count reaches zero.
    pub fn unregister_producer(&self) {
        let mut state = self.state.lock().unwrap();
        state.active_producers = state.active_producers.saturating_sub(1);
        if state.active_producers == 0 {
            state.producers_done = true;
        }
    }
    
    /// Adds a publication to the queue. Blocks with backpressure if queue is full.
    pub fn enqueue(&self, record: PublicationRecord) -> Result<(), String> {
        loop {
            let mut state = self.state.lock().unwrap();
            if state.queue.len() < self.config.max_queue_size {
                state.queue.push_back(record);
                state.last_job_time = Some(Instant::now());
                return Ok(());
            }
            drop(state);
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    
    /// Removes and returns the next publication from the queue, or None if empty.
    pub fn dequeue(&self) -> Option<PublicationRecord> {
        self.state.lock().unwrap().queue.pop_front()
    }
    
    /// Checks stop conditions. Returns Some(reason) if processing should stop.
    pub fn should_stop(&self, start_time: Instant) -> Option<String> {
        let state = self.state.lock().unwrap();
        
        if start_time.elapsed() >= self.config.max_processing_time {
            return Some(format!("Max processing time {:?} exceeded", self.config.max_processing_time));
        }
        
        if state.producers_done && state.queue.is_empty() {
            return Some("All producers finished and queue is empty".to_string());
        }
        
        if let Some(last_job_time) = state.last_job_time {
            if last_job_time.elapsed() >= self.config.idle_timeout {
                return Some(format!("Idle timeout {:?} exceeded", self.config.idle_timeout));
            }
        }
        
        None
    }
    
    pub fn queue_size(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }
    
    pub fn producers_finished(&self) -> bool {
        self.state.lock().unwrap().producers_done
    }
    
    pub fn active_producer_count(&self) -> usize {
        self.state.lock().unwrap().active_producers
    }
    
    /// Creates a RAII producer handle that auto-registers/unregisters.
    pub fn create_producer(&self) -> QueueProducer {
        self.register_producer();
        QueueProducer {
            queue: Arc::clone(&self.state),
        }
    }
}

impl Clone for IngestionQueue {
    /// Clones the Arc reference to share the same underlying queue across threads.
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            config: self.config.clone(),
        }
    }
}

/// RAII handle that auto-unregisters producer on drop, even if scraper panics.
pub struct QueueProducer {
    queue: Arc<Mutex<QueueState>>,
}

impl QueueProducer {
    /// Adds a publication to the queue. Blocks with backpressure if queue is full.
    pub fn submit(&self, record: PublicationRecord) -> Result<(), String> {
        loop {
            let mut state = self.queue.lock().unwrap();
            if state.queue.len() < 10000 {
                state.queue.push_back(record);
                state.last_job_time = Some(Instant::now());
                return Ok(());
            }
            drop(state);
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for QueueProducer {
    /// Automatically unregisters producer. Sets producers_done when last producer drops.
    fn drop(&mut self) {
        let mut state = self.queue.lock().unwrap();
        state.active_producers = state.active_producers.saturating_sub(1);
        if state.active_producers == 0 {
            state.producers_done = true;
        }
    }
}

