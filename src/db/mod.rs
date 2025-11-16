//! Database layer for the Erdős Graph project.
//!
//! This module provides the database interface using IndraDB (a graph database)
//! backed by RocksDB for persistence. It handles:
//! - Database initialization and connection management
//! - Publication ingestion and deduplication
//! - Graph schema definition (Person and Publication vertices, AUTHORED edges)
//! - Checkpointing for incremental scraping
//! - Thread-safe Ingestion queue for publications

pub mod client;
pub mod ingestion;
pub mod ingestion_queue;
pub mod schema;

#[cfg(test)]
pub mod tests;
