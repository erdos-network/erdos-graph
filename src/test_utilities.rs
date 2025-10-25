//! Tests for utility functions.

#[cfg(test)]
mod tests {
    use crate::utilities::{generate_chunks, hash_publication};
    use chrono::{Duration, Utc};

    /// Test generating date chunks with evenly divisible range.
    #[test]
    fn test_generate_chunks_even_division() {
        let start = Utc::now();
        let end = start + Duration::days(30);
        let chunk_size = Duration::days(10);

        let chunks = generate_chunks(start, end, chunk_size);

        assert_eq!(chunks.len(), 3); // 30 days / 10 days = 3 chunks

        // Verify first chunk
        assert_eq!(chunks[0].0, start);
        assert_eq!(chunks[0].1, start + Duration::days(10));

        // Verify last chunk
        assert_eq!(chunks[2].1, end);

        // Verify chunks are contiguous
        for i in 0..chunks.len() - 1 {
            assert_eq!(chunks[i].1, chunks[i + 1].0);
        }
    }

    /// Test generating date chunks with uneven division.
    #[test]
    fn test_generate_chunks_uneven_division() {
        let start = Utc::now();
        let end = start + Duration::days(25);
        let chunk_size = Duration::days(10);

        let chunks = generate_chunks(start, end, chunk_size);

        assert_eq!(chunks.len(), 3); // 3 chunks: 10, 10, 5 days

        // Last chunk should be smaller
        let last_chunk_duration = chunks[2].1 - chunks[2].0;
        assert_eq!(last_chunk_duration, Duration::days(5));
    }

    /// Test generating chunks with empty range.
    #[test]
    fn test_generate_chunks_empty_range() {
        let start = Utc::now();
        let end = start; // Same time
        let chunk_size = Duration::days(7);

        let chunks = generate_chunks(start, end, chunk_size);

        assert_eq!(chunks.len(), 0); // No chunks
    }

    /// Test generating chunks with single small range.
    #[test]
    fn test_generate_chunks_single_small_chunk() {
        let start = Utc::now();
        let end = start + Duration::days(3);
        let chunk_size = Duration::days(10);

        let chunks = generate_chunks(start, end, chunk_size);

        assert_eq!(chunks.len(), 1); // Only one chunk
        assert_eq!(chunks[0].0, start);
        assert_eq!(chunks[0].1, end);
    }

    /// Test generating chunks with very small chunk size.
    #[test]
    fn test_generate_chunks_hourly() {
        let start = Utc::now();
        let end = start + Duration::hours(5);
        let chunk_size = Duration::hours(1);

        let chunks = generate_chunks(start, end, chunk_size);

        assert_eq!(chunks.len(), 5); // 5 hourly chunks
    }

    /// Test publication hash generation.
    #[test]
    fn test_hash_publication_basic() {
        let hash = hash_publication(
            "arxiv:2024.12345",
            "Novel Graph Algorithm",
            &["Alice".to_string(), "Bob".to_string()],
            2024,
        );

        // SHA-256 produces 64 hex characters
        assert_eq!(hash.len(), 64);

        // Hash should be deterministic
        let hash2 = hash_publication(
            "arxiv:2024.12345",
            "Novel Graph Algorithm",
            &["Alice".to_string(), "Bob".to_string()],
            2024,
        );
        assert_eq!(hash, hash2);
    }

    /// Test that different publications produce different hashes.
    #[test]
    fn test_hash_publication_uniqueness() {
        let hash1 = hash_publication(
            "arxiv:2024.12345",
            "Novel Graph Algorithm",
            &["Alice".to_string()],
            2024,
        );

        let hash2 = hash_publication(
            "arxiv:2024.12346", // Different ID
            "Novel Graph Algorithm",
            &["Alice".to_string()],
            2024,
        );

        assert_ne!(hash1, hash2);
    }

    /// Test that author order affects hash.
    #[test]
    fn test_hash_publication_author_order() {
        let hash1 = hash_publication(
            "arxiv:2024.12345",
            "Test Paper",
            &["Alice".to_string(), "Bob".to_string()],
            2024,
        );

        let hash2 = hash_publication(
            "arxiv:2024.12345",
            "Test Paper",
            &["Bob".to_string(), "Alice".to_string()],
            2024,
        );

        // Author order matters (important for detecting true duplicates)
        assert_ne!(hash1, hash2);
    }

    /// Test hash with empty author list.
    #[test]
    fn test_hash_publication_no_authors() {
        let hash = hash_publication("test:123", "Authorless Paper", &[], 2024);

        assert_eq!(hash.len(), 64);
    }

    /// Test hash with special characters in title.
    #[test]
    fn test_hash_publication_special_characters() {
        let hash = hash_publication(
            "test:456",
            "Title with émojis 🎉 and spëcial çhars!",
            &["José García".to_string()],
            2024,
        );

        assert_eq!(hash.len(), 64);
    }

    /// Test that year affects hash.
    #[test]
    fn test_hash_publication_year_sensitivity() {
        let hash1 = hash_publication("test:789", "Same Title", &["Author".to_string()], 2023);

        let hash2 = hash_publication("test:789", "Same Title", &["Author".to_string()], 2024);

        assert_ne!(hash1, hash2);
    }
}
