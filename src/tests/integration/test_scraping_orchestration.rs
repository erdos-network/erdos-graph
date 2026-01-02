use crate::config::CONFIG_LOCK;
use crate::scrapers::scraping_orchestrator::run_scrape;
use chrono::{Duration, Utc};
use indradb::RocksdbDatastore;
use std::env;
use tempfile::TempDir;

#[tokio::test]
async fn test_scraping_flow_arxiv() -> Result<(), Box<dyn std::error::Error>> {
    // Create temporary directory for the database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db_arxiv.rocksdb");

    // Initialize RocksDB datastore
    let mut database = RocksdbDatastore::new_db(db_path)?;

    // Define short time range to keep the test fast
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(1);

    let sources = vec!["arxiv".to_string()];

    // Acquire lock for config file access
    let guard = CONFIG_LOCK.lock().unwrap();

    // Config setup
    let config_content = r#"{
        "scrapers": {
            "enabled": ["arxiv"]
        },
        "ingestion": {
            "chunk_size_days": 1,
            "initial_start_date": "2020-01-01T00:00:00Z",
            "weekly_days": 7
        },
        "deduplication": {
            "title_similarity_threshold": 0.9,
            "author_similarity_threshold": 0.5
        },
        "heartbeat_timeout_s": 30,
        "polling_interval_ms": 100
    }"#;

    let config_path = env::current_dir()?.join("config.json");
    let config_existed = config_path.exists();

    if config_existed {
        let backup_path = env::current_dir()?.join("config.json.bak");
        std::fs::copy(&config_path, &backup_path)?;
    }
    std::fs::write(&config_path, config_content)?;

    // Release lock before running scrape to avoid deadlock
    drop(guard);

    let result = run_scrape(start_date, end_date, sources, &mut database).await;

    if config_existed {
        let backup_path = env::current_dir()?.join("config.json.bak");
        tokio::fs::copy(&backup_path, &config_path).await?;
        tokio::fs::remove_file(backup_path).await?;
    } else {
        tokio::fs::remove_file(&config_path).await?;
    }

    assert!(result.is_ok(), "Scraping failed: {:?}", result.err());

    Ok(())
}

#[tokio::test]
async fn test_scraping_flow_dblp() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db_dblp.rocksdb");
    let mut database = RocksdbDatastore::new_db(db_path)?;

    // Config setup
    let config_content = r#"{
        "scrapers": {
            "enabled": ["dblp"]
        },
        "ingestion": {
            "chunk_size_days": 1,
            "initial_start_date": "2020-01-01T00:00:00Z",
            "weekly_days": 7
        },
        "deduplication": {
            "title_similarity_threshold": 0.9,
            "author_similarity_threshold": 0.5
        },
        "heartbeat_timeout_s": 30,
        "polling_interval_ms": 100
    }"#;

    let config_path = env::current_dir()?.join("config.json");

    // Acquire lock for config file access
    let guard = CONFIG_LOCK.lock().unwrap();
    std::fs::write(&config_path, config_content)?;

    // Release lock before running scrape to avoid deadlock
    drop(guard);

    // Use very recent short range to minimize processing time
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(1);
    let sources = vec!["dblp".to_string()];

    let result = run_scrape(start_date, end_date, sources, &mut database).await;

    assert!(result.is_ok(), "DBLP scraping failed: {:?}", result.err());

    Ok(())
}
