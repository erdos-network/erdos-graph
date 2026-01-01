#![coverage(off)]

use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct Config {
    pub scrapers: ScraperConfig,
    pub ingestion: IngestionConfig,
    pub deduplication: DeduplicationConfig,
    pub heartbeat_timeout_s: u64,
    pub polling_interval_ms: u64,
}

#[derive(Deserialize)]
pub struct DeduplicationConfig {
    pub title_similarity_threshold: f64,
    pub author_similarity_threshold: f64,
}

#[derive(Deserialize)]
pub struct IngestionConfig {
    /// Size of each processing chunk in days (e.g., 1 for daily chunks)
    pub chunk_size_days: u64,
    /// Start date for "initial" mode (ISO 8601 format, e.g., "1932-01-01T00:00:00Z")
    pub initial_start_date: String,
    /// Number of days to scrape back for "weekly" mode
    pub weekly_days: u64,
}

#[derive(Deserialize)]
pub struct ScraperConfig {
    pub enabled: Vec<String>,
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let content = fs::read_to_string("config.json")?;
    serde_json::from_str(&content).map_err(Into::into)
}
