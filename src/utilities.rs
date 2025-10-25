//! Utility functions for date chunking and publication deduplication.
//!
//! This module provides helper functions used throughout the scraping and ingestion process.

use chrono::{DateTime, Duration, Utc};

/// Divides a date range into smaller chunks of a specified size.
///
/// This is useful for breaking large scraping jobs into manageable pieces,
/// allowing for checkpointing and avoiding memory issues with large result sets.
///
/// # Arguments
/// * `start` - The beginning of the overall date range
/// * `end` - The end of the overall date range
/// * `chunk_size` - The maximum duration of each chunk
///
/// # Returns
/// A vector of (start, end) date pairs representing sequential chunks.
/// The last chunk may be smaller than `chunk_size` if the range doesn't divide evenly.
///
/// # Example
/// ```
/// use chrono::{Duration, Utc};
/// use erdos_graph::utilities::generate_chunks;
///
/// let start = Utc::now();
/// let end = start + Duration::days(30);
/// let chunks = generate_chunks(start, end, Duration::days(7));
/// assert_eq!(chunks.len(), 5); // 30 days / 7 days per chunk = 5 chunks
/// ```
pub fn generate_chunks(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    chunk_size: Duration,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    let mut chunks = Vec::new();
    let mut current = start;
    while current < end {
        let next = (current + chunk_size).min(end);
        chunks.push((current, next));
        current = next;
    }
    chunks
}

/// Generates a SHA-256 hash for a publication record to enable deduplication.
///
/// This hash can be used to identify duplicate publications from different sources.
/// The hash is computed from the publication ID, title, concatenated author names, and year.
///
/// # Arguments
/// * `id` - The source-specific publication ID (e.g., ArXiv ID, DBLP key)
/// * `title` - The publication title
/// * `authors` - A slice of author names
/// * `year` - The publication year
///
/// # Returns
/// A hexadecimal string representation of the SHA-256 hash
///
/// # Example
/// ```
/// use erdos_graph::utilities::hash_publication;
///
/// let hash = hash_publication(
///     "arxiv:2024.12345",
///     "Graph Theory Applications",
///     &["Alice".to_string(), "Bob".to_string()],
///     2024
/// );
/// assert_eq!(hash.len(), 64); // SHA-256 produces 64 hex characters
/// ```
pub fn hash_publication(id: &str, title: &str, authors: &[String], year: u32) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(format!("{}{}{}{}", id, title, authors.join(","), year));
    format!("{:x}", hasher.finalize())
}
