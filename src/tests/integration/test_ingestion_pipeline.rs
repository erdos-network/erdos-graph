//! Integration tests for the complete ingestion pipeline.
//!
//! These tests exercise the full pipeline from `orchestrate_scraping_and_ingestion`
//! through multiprocessed scraping, checkpointing, chunking, and database ingestion.

use crate::config::{Config, DeduplicationConfig, IngestionConfig, ScraperConfig};
use crate::db::ingestion::{get_checkpoint, orchestrate_scraping_and_ingestion, set_checkpoint};
use chrono::{Duration, Utc};
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use helix_db::helix_engine::traversal_core::HelixGraphEngineOpts;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

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

        // Start mock ArXiv server
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"
                <feed xmlns="http://www.w3.org/2005/Atom">
                    <title>ArXiv Query: search_query=all:electron</title>
                    <id>http://arxiv.org/api/111</id>
                    <updated>2020-01-01T00:00:00Z</updated>
                    <opensearch:totalResults xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:totalResults>
                    <opensearch:startIndex xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:startIndex>
                    <opensearch:itemsPerPage xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">10</opensearch:itemsPerPage>
                </feed>
            "#))
            .mount(&mock_server)
            .await;

        let sources = vec!["arxiv".to_string()];

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: (Utc::now() - Duration::days(2)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        // Point to mock server and disable delays
        config.scrapers.arxiv.base_url = mock_server.uri().to_string();
        config.scrapers.arxiv.delay_ms = 0;

        let result =
            orchestrate_scraping_and_ingestion("initial", sources.clone(), None, database, &config)
                .await;

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

        // Start mock server
        let mock_server = MockServer::start().await;

        // Mock DBLP empty response
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {
                    "hits": {
                        "hit": [],
                        "@sent": "0",
                        "@total": "0"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        let sources = vec!["dblp".to_string()];

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1, // Reduced from 7
                initial_start_date: (Utc::now() - Duration::days(2)).to_rfc3339(), // Reduced range
                weekly_days: 1,     // Reduced from 7
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        // Point to mock server and disable delays
        config.scrapers.dblp.base_url = mock_server.uri().to_string();
        config.scrapers.dblp.delay_ms = 0;
        config.scrapers.dblp.retry_delay_ms = 0;
        config.scrapers.dblp.long_pause_ms = 0;

        let checkpoint_date = Utc::now() - Duration::hours(1);
        set_checkpoint("dblp", checkpoint_date, &checkpoint_dir)?;

        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), None, database, &config)
                .await;

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

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {
                    "hits": {
                        "hit": [],
                        "@sent": "0",
                        "@total": "0"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        let mock_arxiv_server = MockServer::start().await;
        Mock::given(method("GET"))
             .respond_with(ResponseTemplate::new(200).set_body_string(r#"
                 <feed xmlns="http://www.w3.org/2005/Atom">
                    <title>ArXiv Query: search_query=all:electron</title>
                    <id>http://arxiv.org/api/111</id>
                    <updated>2020-01-01T00:00:00Z</updated>
                    <opensearch:totalResults xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:totalResults>
                    <opensearch:startIndex xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:startIndex>
                    <opensearch:itemsPerPage xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">10</opensearch:itemsPerPage>
                 </feed>
             "#))
             .mount(&mock_arxiv_server)
             .await;

        let sources = vec!["arxiv".to_string(), "dblp".to_string()];

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1, // Reduced from 7
                initial_start_date: (Utc::now() - Duration::days(2)).to_rfc3339(), // Reduced range
                weekly_days: 1,     // Reduced from 1
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        config.scrapers.dblp.base_url = mock_server.uri().to_string();
        config.scrapers.dblp.delay_ms = 0;
        config.scrapers.dblp.long_pause_ms = 0;
        config.scrapers.dblp.retry_delay_ms = 0;

        config.scrapers.arxiv.base_url = mock_arxiv_server.uri().to_string();
        config.scrapers.arxiv.delay_ms = 0;

        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), None, database, &config)
                .await;

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

        // Start mock ArXiv server
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"
                <feed xmlns="http://www.w3.org/2005/Atom">
                    <title>ArXiv Query: search_query=all:electron</title>
                    <id>http://arxiv.org/api/111</id>
                    <updated>2020-01-01T00:00:00Z</updated>
                    <opensearch:totalResults xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:totalResults>
                    <opensearch:startIndex xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:startIndex>
                    <opensearch:itemsPerPage xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">10</opensearch:itemsPerPage>
                </feed>
            "#))
            .mount(&mock_server)
            .await;

        let sources = vec!["arxiv".to_string()];

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 2,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        // Point to mock server and disable delays
        config.scrapers.arxiv.base_url = mock_server.uri().to_string();
        config.scrapers.arxiv.delay_ms = 0;

        let checkpoint_before = get_checkpoint("arxiv", &checkpoint_dir)?;
        assert!(
            checkpoint_before.is_none(),
            "Checkpoint should not exist initially"
        );

        let result1 = orchestrate_scraping_and_ingestion(
            "weekly",
            sources.clone(),
            None,
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
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), None, database, &config)
                .await;
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

        // Start mock ArXiv server
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"
                <feed xmlns="http://www.w3.org/2005/Atom">
                    <title>ArXiv Query: search_query=all:electron</title>
                    <id>http://arxiv.org/api/111</id>
                    <updated>2020-01-01T00:00:00Z</updated>
                    <opensearch:totalResults xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:totalResults>
                    <opensearch:startIndex xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:startIndex>
                    <opensearch:itemsPerPage xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">10</opensearch:itemsPerPage>
                </feed>
            "#))
            .mount(&mock_server)
            .await;

        let sources = vec!["arxiv".to_string()];

        let start_checkpoint = Utc::now() - Duration::days(3);
        set_checkpoint("arxiv", start_checkpoint, &checkpoint_dir)?;

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        // Point to mock server and disable delays
        config.scrapers.arxiv.base_url = mock_server.uri().to_string();
        config.scrapers.arxiv.delay_ms = 0;

        let result =
            orchestrate_scraping_and_ingestion("weekly", sources.clone(), None, database, &config)
                .await;

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

        // Start mock ArXiv server
        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"
                <feed xmlns="http://www.w3.org/2005/Atom">
                    <title>ArXiv Query: search_query=all:electron</title>
                    <id>http://arxiv.org/api/111</id>
                    <updated>2020-01-01T00:00:00Z</updated>
                    <opensearch:totalResults xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:totalResults>
                    <opensearch:startIndex xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:startIndex>
                    <opensearch:itemsPerPage xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">10</opensearch:itemsPerPage>
                </feed>
            "#))
            .mount(&mock_server)
            .await;

        let sources = vec!["arxiv".to_string()];

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 365,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        // Point to mock server and disable delays
        config.scrapers.arxiv.base_url = mock_server.uri().to_string();
        config.scrapers.arxiv.delay_ms = 0;

        let recent_checkpoint = Utc::now() - Duration::days(1);
        set_checkpoint("arxiv", recent_checkpoint, &checkpoint_dir)?;

        let result =
            orchestrate_scraping_and_ingestion("full", sources.clone(), None, database, &config)
                .await;

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

        let result = orchestrate_scraping_and_ingestion(
            "invalid_mode",
            sources.clone(),
            None,
            database,
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

    #[tokio::test]
    async fn test_dblp_xml_mode_scraping() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        let download_dir = temp_dir.path().join("xml_downloads");
        std::fs::create_dir_all(&checkpoint_dir)?;

        // Setup mock server for XML download
        let mock_server = MockServer::start().await;

        // Create a minimal valid DBLP XML gz file
        let xml_content = r#"<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/1">
<author>Test Author</author>
<title>Test Title</title>
<year>2024</year>
<journal>Test Journal</journal>
</article>
</dblp>"#;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, xml_content.as_bytes())?;
        let gz_content = encoder.finish()?;

        Mock::given(method("GET"))
            .and(wiremock::matchers::path("/xml/dblp.xml.gz"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(gz_content))
            .mount(&mock_server)
            .await;

        let database = create_test_datastore().await;

        let sources = vec!["dblp".to_string()];
        let mut source_modes = HashMap::new();
        source_modes.insert("dblp".to_string(), "xml".to_string());

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 7,
                initial_start_date: (Utc::now() - Duration::days(14)).to_rfc3339(),
                weekly_days: 7,
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        config.scrapers.dblp.xml_base_url = format!("{}/xml", mock_server.uri());
        config.scrapers.dblp.xml_download_dir = download_dir.to_str().unwrap().to_string();

        let result = orchestrate_scraping_and_ingestion(
            "weekly",
            sources.clone(),
            Some(source_modes),
            database,
            &config,
        )
        .await;

        assert!(
            result.is_ok(),
            "XML mode scraping should work with mock: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dblp_search_mode_explicit() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {
                    "hits": {
                        "hit": [],
                        "@sent": "0",
                        "@total": "0"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        let sources = vec!["dblp".to_string()];
        let mut source_modes = HashMap::new();
        source_modes.insert("dblp".to_string(), "search".to_string());

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1, // Reduced from 7
                initial_start_date: (Utc::now() - Duration::days(2)).to_rfc3339(), // Reduced range
                weekly_days: 1,     // Reduced from 7
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        config.scrapers.dblp.base_url = mock_server.uri().to_string();
        config.scrapers.dblp.delay_ms = 0;
        config.scrapers.dblp.long_pause_ms = 0;
        config.scrapers.dblp.retry_delay_ms = 0;

        let result = orchestrate_scraping_and_ingestion(
            "weekly",
            sources.clone(),
            Some(source_modes),
            database,
            &config,
        )
        .await;

        assert!(
            result.is_ok(),
            "Search mode scraping should work: {:?}",
            result.err()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_sources_with_dblp_mode() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let checkpoint_dir = temp_dir.path().join("checkpoints");
        std::fs::create_dir_all(&checkpoint_dir)?;

        let database = create_test_datastore().await;

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {
                    "hits": {
                        "hit": [],
                        "@sent": "0",
                        "@total": "0"
                    }
                }
            })))
            .mount(&mock_server)
            .await;

        // Mock Arxiv as well since it's used here
        let mock_arxiv_server = MockServer::start().await;
        Mock::given(method("GET"))
             .respond_with(ResponseTemplate::new(200).set_body_string(r#"
                 <feed xmlns="http://www.w3.org/2005/Atom">
                    <title>ArXiv Query: search_query=all:electron</title>
                    <id>http://arxiv.org/api/111</id>
                    <updated>2020-01-01T00:00:00Z</updated>
                    <opensearch:totalResults xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:totalResults>
                    <opensearch:startIndex xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">0</opensearch:startIndex>
                    <opensearch:itemsPerPage xmlns:opensearch="http://a9.com/-/spec/opensearch/1.1/">10</opensearch:itemsPerPage>
                 </feed>
             "#))
             .mount(&mock_arxiv_server)
             .await;

        let sources = vec!["arxiv".to_string(), "dblp".to_string()];
        let mut source_modes = HashMap::new();
        // Only specify mode for dblp, arxiv will use default
        source_modes.insert("dblp".to_string(), "search".to_string());

        let mut config = Config {
            scrapers: ScraperConfig {
                enabled: sources.clone(),
                dblp: Default::default(),
                arxiv: Default::default(),
                zbmath: Default::default(),
            },
            ingestion: IngestionConfig {
                chunk_size_days: 1, // Reduced from 7
                initial_start_date: (Utc::now() - Duration::days(2)).to_rfc3339(), // Reduced range
                weekly_days: 1,     // Reduced from 1 (was 1 already but keep consistent)
                checkpoint_dir: Some(checkpoint_dir.to_str().unwrap().to_string()),
                ..Default::default()
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

        config.scrapers.dblp.base_url = mock_server.uri().to_string();
        config.scrapers.dblp.delay_ms = 0;
        config.scrapers.dblp.long_pause_ms = 0;
        config.scrapers.dblp.retry_delay_ms = 0;

        config.scrapers.arxiv.base_url = mock_arxiv_server.uri().to_string();
        config.scrapers.arxiv.delay_ms = 0;

        let result = orchestrate_scraping_and_ingestion(
            "weekly",
            sources.clone(),
            Some(source_modes),
            database,
            &config,
        )
        .await;

        assert!(
            result.is_ok(),
            "Mixed source scraping with mode should work: {:?}",
            result.err()
        );

        Ok(())
    }
}
