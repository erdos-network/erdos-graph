use crate::config::{Config, DeduplicationConfig, IngestionConfig, ScraperConfig};
use crate::scrapers::scraping_orchestrator::run_scrape;
use chrono::{Duration, Utc};
use helix_db::helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts};
use std::sync::Arc;
use tempfile::TempDir;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[allow(clippy::field_reassign_with_default)]
    async fn test_scraping_flow_arxiv() -> Result<(), Box<dyn std::error::Error>> {
        // Create temporary directory for the database
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_arxiv.helix");
        std::fs::create_dir_all(&db_path)?;

        // Initialize Helix datastore
        let mut opts = HelixGraphEngineOpts::default();
        opts.path = db_path.to_string_lossy().to_string();
        let engine = Arc::new(HelixGraphEngine::new(opts)?);

        // Define short time range to keep the test fast
        let end_date = Utc::now();
        let start_date = end_date - Duration::days(1);

        let sources = vec!["arxiv".to_string()];

        // Config setup
        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec!["arxiv".to_string()],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        let result = run_scrape(start_date, end_date, sources, engine, &config).await;

        assert!(result.is_ok(), "Scraping failed: {:?}", result.err());

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::field_reassign_with_default)]
    async fn test_scraping_flow_dblp() -> Result<(), Box<dyn std::error::Error>> {
        // Create a temporary database
        let temp_dir = TempDir::new()?;
        let db_path = temp_dir.path().join("test_db_dblp.helix");
        std::fs::create_dir_all(&db_path)?;

        let mut opts = HelixGraphEngineOpts::default();
        opts.path = db_path.to_string_lossy().to_string();
        let engine = Arc::new(HelixGraphEngine::new(opts)?);

        // Config setup
        let config = Config {
            scrapers: ScraperConfig {
                enabled: vec!["dblp".to_string()],
                dblp: Default::default(),
                arxiv: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: "2020-01-01T00:00:00Z".to_string(),
                weekly_days: 7,
                checkpoint_dir: None,
            },
            deduplication: DeduplicationConfig {
                title_similarity_threshold: 0.9,
                author_similarity_threshold: 0.5,
                bloom_filter_size: 100,
            },
            edge_cache: Default::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
            log_level: crate::logger::LogLevel::Info,
        };

        // Use very recent short range to minimize processing time
        let end_date = Utc::now();
        let start_date = end_date - Duration::days(1);
        let sources = vec!["dblp".to_string()];

        let result = run_scrape(start_date, end_date, sources, engine, &config).await;

        assert!(result.is_ok(), "DBLP scraping failed: {:?}", result.err());

        Ok(())
    }
}
