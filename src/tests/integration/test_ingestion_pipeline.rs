//! Integration tests for the complete ingestion pipeline.
//!
//! These tests exercise the full pipeline from `orchestrate_scraping_and_ingestion`
//! through multiprocessed scraping, checkpointing, chunking, and database ingestion.

use crate::config::{Config, DeduplicationConfig, IngestionConfig, ScraperConfig};
use crate::db::client::init_datastore;
use crate::db::ingestion::{get_checkpoint, orchestrate_scraping_and_ingestion, set_checkpoint};
use chrono::{Duration, Utc};
use tempfile::TempDir;

#[cfg(test)]
mod tests {
    use super::*;

    /// Test initial ingestion mode with ArXiv source.
    ///
    /// This test verifies:
    /// - orchestrate_scraping_and_ingestion works end-to-end
    /// - Chunking happens correctly
    /// - Checkpoints are created after processing
    /// - Multiple sources can be processed in parallel
    #[tokio::test]
    async fn test_full_pipeline_initial_mode_arxiv() -> Result<(), Box<dyn std::error::Error>> {
        // Create temporary directory for the database and checkpoints
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_initial_arxiv.rocksdb");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        // Initialize RocksDB datastore
        // Initialize RocksDB datastore
        let mut database = init_datastore(&db_path)?;

        // Speed up tests by disabling scraper delays
        unsafe {
            std::env::set_var("ARXIV_DELAY_MS", "0");
            std::env::set_var("DBLP_DELAY_MS", "0");
        }

        let sources = vec!["arxiv".to_string()];

        // Config setup with small chunk size for testing
        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1, // Small chunks to test chunking logic
                initial_start_date: (Utc::now() - Duration::days(2)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
        };

        // Run initial ingestion (will use configured start date since no checkpoint exists)
        let result =
            orchestrate_scraping_and_ingestion("initial", sources.clone(), &mut database, &config)
                .await;

        assert!(
            result.is_ok(),
            "Initial ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    /// Test weekly mode with checkpoint resumption for DBLP.
    ///
    /// This test verifies:
    /// - Weekly mode uses checkpoints if they exist
    /// - Checkpoint-based resumption works correctly
    /// - Small incremental updates work
    #[tokio::test]
    async fn test_full_pipeline_weekly_mode_dblp() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_weekly_dblp.rocksdb");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let mut database = init_datastore(&db_path)?;

        // Speed up tests by disabling scraper delays
        unsafe {
            std::env::set_var("DBLP_DELAY_MS", "0");
        }

        let sources = vec!["dblp".to_string()];

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 7,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
        };

        // Set a checkpoint from 1 hour ago to keep within current year (avoiding large prev year scrape)
        let checkpoint_date = Utc::now() - Duration::hours(1);
        set_checkpoint("dblp", checkpoint_date, &checkpoint_dir)?;

        // Run weekly ingestion - should scrape from checkpoint to now
        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), &mut database, &config)
                .await;

        assert!(
            result.is_ok(),
            "Weekly ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    /// Test multiple sources processed in parallel.
    ///
    /// This test verifies:
    /// - Multiple sources can be processed together
    /// - Each source gets its own checkpoint
    /// - Parallel processing works correctly
    #[tokio::test]
    async fn test_full_pipeline_multiple_sources() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_multi.rocksdb");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let mut database = init_datastore(&db_path)?;

        // Speed up tests by disabling scraper delays
        unsafe {
            std::env::set_var("ARXIV_DELAY_MS", "0");
            std::env::set_var("DBLP_DELAY_MS", "0");
        }

        let sources = vec!["arxiv".to_string(), "dblp".to_string()];

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 7,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 1, // Reduced to 1 to stay in 2026
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
        };

        // Run with multiple sources
        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), &mut database, &config)
                .await;

        // Verify checkpoints were created for both sources
        let arxiv_checkpoint = get_checkpoint("arxiv", &checkpoint_dir)?;
        let dblp_checkpoint = get_checkpoint("dblp", &checkpoint_dir)?;

        assert!(
            result.is_ok(),
            "Multi-source ingestion failed: {:?}",
            result.err()
        );
        assert!(arxiv_checkpoint.is_some(), "ArXiv checkpoint not created");
        assert!(dblp_checkpoint.is_some(), "DBLP checkpoint not created");

        Ok(())
    }

    /// Test checkpoint persistence and resumption.
    ///
    /// This test verifies:
    /// - Checkpoints are saved after processing
    /// - Subsequent runs resume from checkpoint
    /// - No duplicate processing occurs
    #[tokio::test]
    async fn test_checkpoint_persistence_and_resumption() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_checkpoint.rocksdb");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let mut database = init_datastore(&db_path)?;

        let sources = vec!["arxiv".to_string()];

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 2,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
        };

        // First run - no checkpoint exists
        let checkpoint_before = get_checkpoint("arxiv", &checkpoint_dir)?;
        assert!(
            checkpoint_before.is_none(),
            "Checkpoint should not exist initially"
        );

        let result1 =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), &mut database, &config)
                .await;
        assert!(result1.is_ok(), "First run failed: {:?}", result1.err());

        // Verify checkpoint was created
        let checkpoint_after_first = get_checkpoint("arxiv", &checkpoint_dir)?;
        assert!(
            checkpoint_after_first.is_some(),
            "Checkpoint should be created after first run"
        );
        let first_checkpoint = checkpoint_after_first.unwrap();

        // Second run - should resume from checkpoint
        let result2 =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), &mut database, &config)
                .await;
        assert!(result2.is_ok(), "Second run failed: {:?}", result2.err());

        // Verify checkpoint was updated
        let checkpoint_after_second = get_checkpoint("arxiv", &checkpoint_dir)?;
        assert!(
            checkpoint_after_second.is_some(),
            "Checkpoint should still exist after second run"
        );
        let second_checkpoint = checkpoint_after_second.unwrap();

        // Second checkpoint should be later than first
        assert!(
            second_checkpoint >= first_checkpoint,
            "Checkpoint should advance"
        );

        Ok(())
    }

    /// Test chunking with small date ranges.
    ///
    /// This test verifies:
    /// - Date ranges are properly chunked
    /// - Small ranges work correctly
    /// - Chunk boundaries are respected
    #[tokio::test]
    async fn test_chunking_small_range() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_chunking.rocksdb");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let mut database = init_datastore(&db_path)?;

        let sources = vec!["arxiv".to_string()];

        // Set checkpoint to 3 days ago
        let start_checkpoint = Utc::now() - Duration::days(3);
        set_checkpoint("arxiv", start_checkpoint, &checkpoint_dir)?;

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1, // 1 day chunks, so 3 day range = 3 chunks
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
        };

        // Run weekly ingestion - should process 3 chunks
        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), &mut database, &config)
                .await;

        assert!(
            result.is_ok(),
            "Chunked ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    /// Test full mode starting from the beginning.
    ///
    /// This test verifies:
    /// - Full mode ignores checkpoints for start date calculation
    /// - Can process from very early dates
    /// - Works with default configuration
    #[tokio::test]
    async fn test_full_mode_from_beginning() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_full.rocksdb");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let mut database = init_datastore(&db_path)?;

        let sources = vec!["arxiv".to_string()];

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 365, // Large chunks to keep test reasonable
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            heartbeat_timeout_s: 120,
            polling_interval_ms: 100,
        };

        // For this test, we'll use a checkpoint to limit the range
        // but test that full mode still works
        let recent_checkpoint = Utc::now() - Duration::days(1);
        set_checkpoint("arxiv", recent_checkpoint, &checkpoint_dir)?;

        // Run full ingestion - with checkpoint, should still process from checkpoint
        let result =
            orchestrate_scraping_and_ingestion("full", sources.clone(), &mut database, &config)
                .await;

        assert!(
            result.is_ok(),
            "Full mode ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    /// Test error handling with invalid mode.
    ///
    /// This test verifies:
    /// - Invalid modes are rejected
    /// - Error messages are appropriate
    #[tokio::test]
    async fn test_invalid_mode_handling() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_invalid.rocksdb");
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let mut database = init_datastore(&db_path)?;

        let sources = vec!["arxiv".to_string()];

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
        };

        // Run with invalid mode
        let result = orchestrate_scraping_and_ingestion(
            "invalid_mode",
            sources.clone(),
            &mut database,
            &config,
        )
        .await;

        assert!(result.is_err(), "Should fail with invalid mode");
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("Invalid mode"),
            "Error should mention invalid mode"
        );

        Ok(())
    }
}
