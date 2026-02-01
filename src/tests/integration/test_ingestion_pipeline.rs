//! Integration tests for the complete ingestion pipeline.
//!
//! These tests exercise the full pipeline from `orchestrate_scraping_and_ingestion`
//! through multiprocessed scraping, checkpointing, chunking, and database ingestion.

use crate::config::{Config, DeduplicationConfig, IngestionConfig, ScraperConfig};
use crate::db::ingestion::{get_checkpoint, orchestrate_scraping_and_ingestion, set_checkpoint};
use chrono::{Duration, Utc};
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use helix_db::helix_engine::traversal_core::HelixGraphEngineOpts;
use std::sync::Arc;
use tempfile::TempDir;

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::field_reassign_with_default)]
    async fn create_test_datastore() -> Arc<HelixGraphEngine> {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db.helix");
        std::fs::create_dir_all(&db_path).unwrap();

        // Initialize Helix datastore
        let mut opts = HelixGraphEngineOpts::default();
        opts.path = db_path.to_string_lossy().to_string();

        // We leak the temp_dir to keep the files for the duration of the test logic
        // In a real test we might want better cleanup, but tempfile cleans up when the object drops.
        // Here we just want the path.
        std::mem::forget(temp_dir);

        Arc::new(HelixGraphEngine::new(opts).unwrap())
    }

    #[tokio::test]
    async fn test_full_pipeline_initial_mode_arxiv() -> Result<(), Box<dyn std::error::Error>> {
        // Create temporary directory for checkpoints
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

        // Speed up tests by disabling scraper delays
        unsafe {
            std::env::set_var("ARXIV_DELAY_MS", "0");
            std::env::set_var("DBLP_DELAY_MS", "0");
        }

        let sources = vec!["arxiv".to_string()];

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: (Utc::now() - Duration::days(2)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        let result =
            orchestrate_scraping_and_ingestion("initial", sources.clone(), database, &config).await;

        assert!(
            result.is_ok(),
            "Initial ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_full_pipeline_weekly_mode_dblp() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

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
            edge_cache: Default::default(),
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        let checkpoint_date = Utc::now() - Duration::hours(1);
        set_checkpoint("dblp", checkpoint_date, &checkpoint_dir)?;

        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), database, &config).await;

        assert!(
            result.is_ok(),
            "Weekly ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_full_pipeline_multiple_sources() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

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
                weekly_days: 1,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), database, &config).await;

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

    #[tokio::test]
    async fn test_checkpoint_persistence_and_resumption() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

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
            edge_cache: Default::default(),
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        let checkpoint_before = get_checkpoint("arxiv", &checkpoint_dir)?;
        assert!(
            checkpoint_before.is_none(),
            "Checkpoint should not exist initially"
        );

        let result1 = orchestrate_scraping_and_ingestion(
            "weekly",
            sources.clone(),
            database.clone(),
            &config,
        )
        .await;
        assert!(result1.is_ok(), "First run failed: {:?}", result1.err());

        let checkpoint_after_first = get_checkpoint("arxiv", &checkpoint_dir)?;
        assert!(
            checkpoint_after_first.is_some(),
            "Checkpoint should be created after first run"
        );
        let first_checkpoint = checkpoint_after_first.unwrap();

        let result2 =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), database, &config).await;
        assert!(result2.is_ok(), "Second run failed: {:?}", result2.err());

        let checkpoint_after_second = get_checkpoint("arxiv", &checkpoint_dir)?;
        assert!(
            checkpoint_after_second.is_some(),
            "Checkpoint should still exist after second run"
        );
        let second_checkpoint = checkpoint_after_second.unwrap();

        assert!(
            second_checkpoint >= first_checkpoint,
            "Checkpoint should advance"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_chunking_small_range() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

        let sources = vec!["arxiv".to_string()];

        let start_checkpoint = Utc::now() - Duration::days(3);
        set_checkpoint("arxiv", start_checkpoint, &checkpoint_dir)?;

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
            edge_cache: Default::default(),
            heartbeat_timeout_s: 60,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), database, &config).await;

        assert!(
            result.is_ok(),
            "Chunked ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_full_mode_from_beginning() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

        let sources = vec!["arxiv".to_string()];

        let config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 365,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 120,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        let recent_checkpoint = Utc::now() - Duration::days(1);
        set_checkpoint("arxiv", recent_checkpoint, &checkpoint_dir)?;

        let result =
            orchestrate_scraping_and_ingestion("full", sources.clone(), database, &config).await;

        assert!(
            result.is_ok(),
            "Full mode ingestion failed: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_mode_handling() -> Result<(), Box<dyn std::error::Error>> {
        let database = create_test_datastore().await;
        let sources = vec!["arxiv".to_string()];
        let config = Config::default();

        let result =
            orchestrate_scraping_and_ingestion("invalid_mode", sources.clone(), database, &config)
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
