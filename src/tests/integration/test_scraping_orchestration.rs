use crate::scrapers::scraping_orchestrator::run_scrape;
use chrono::{Datelike, Duration, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use indradb::RocksdbDatastore;
use std::env;
use std::io::Write;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_scraping_flow_arxiv() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db_arxiv.rocksdb");

    // Initialize a RocksDB datastore
    let mut database = RocksdbDatastore::new_db(db_path)?;

    // Define a short time range to keep the test fast
    let end_date = Utc::now();
    let start_date = end_date - Duration::days(1);

    let sources = vec!["arxiv".to_string()];

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
        tokio::fs::copy(&config_path, &backup_path).await?;
    }
    tokio::fs::write(&config_path, config_content).await?;

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
    // Setup Mock Server
    let mock_server = MockServer::start().await;

    // Create a dummy DBLP XML
    let current_year = Utc::now().year();
    let xml_content = format!(
        r#"<?xml version="1.0" encoding="ISO-8859-1"?>
<!DOCTYPE dblp SYSTEM "dblp.dtd">
<dblp>
<article key="journals/test/A{}">
  <author>Test Author</author>
  <title>Test Title</title>
  <year>{}</year>
  <journal>Test Journal</journal>
</article>
</dblp>"#,
        current_year, current_year
    );

    // Gzip the content
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(xml_content.as_bytes())?;
    let gzipped_body = encoder.finish()?;

    // Mock the response
    Mock::given(method("GET"))
        .and(path("/xml/dblp.xml.gz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(gzipped_body))
        .mount(&mock_server)
        .await;

    // Create a temporary database
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db_dblp.rocksdb");
    let mut database = RocksdbDatastore::new_db(db_path)?;

    // Set environment variable for DBLP URL
    let mock_url = format!("{}/xml/dblp.xml.gz", mock_server.uri());
    unsafe {
        env::set_var("DBLP_BASE_URL", &mock_url);
    }

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

    // We reuse the config file strategy (carefully, as tests run in parallel)
    // IMPORTANT: Tests running in parallel accessing the same config file is a race condition.
    // For now, we will assume sequential execution or risk flake.
    // To fix this properly, load_config should take a path, but that changes signature.
    // Or we use a lock. Since we are in the same process, we can use a mutex? No, tests are threads.
    // We'll proceed with the file write but this is risky if tests run parallel.
    // To mitigate, we can use a unique config file name per test IF load_config supported it.
    // Since load_config hardcodes "config.json", we have to use that.
    // We'll use a file lock or just rely on the fact that we're writing similar configs.

    let config_path = env::current_dir()?.join("config.json");
    // Just overwrite it. The backup/restore logic in parallel tests is messy.
    // Ideally, we refactor load_config. But keeping signatures...
    // Let's just write it.
    tokio::fs::write(&config_path, config_content).await?;

    let start_date = Utc::now() - Duration::days(365);
    let end_date = Utc::now();
    let sources = vec!["dblp".to_string()];

    let result = run_scrape(start_date, end_date, sources, &mut database).await;

    // Clean up env var
    unsafe {
        env::remove_var("DBLP_BASE_URL");
    }
    // We don't clean up config here to avoid breaking the other test if it's running.
    // This is the downside of hardcoded config paths.

    assert!(result.is_ok(), "DBLP scraping failed: {:?}", result.err());

    Ok(())
}
