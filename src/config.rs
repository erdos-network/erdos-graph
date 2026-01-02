#![coverage(off)]

use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub scrapers: ScraperConfig,
    pub ingestion: IngestionConfig,
    pub deduplication: DeduplicationConfig,
    pub heartbeat_timeout_s: u64,
    pub polling_interval_ms: u64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct DeduplicationConfig {
    pub title_similarity_threshold: f64,
    pub author_similarity_threshold: f64,
}

#[derive(Deserialize, Clone, Debug)]
pub struct IngestionConfig {
    /// Size of each processing chunk in days (e.g., 1 for daily chunks)
    pub chunk_size_days: u64,
    /// Start date for "initial" mode (ISO 8601 format, e.g., "1932-01-01T00:00:00Z")
    pub initial_start_date: String,
    /// Number of days to scrape back for "weekly" mode
    pub weekly_days: u64,
    /// Directory to store checkpoint files (defaults to "checkpoints")
    pub checkpoint_dir: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ScraperConfig {
    pub enabled: Vec<String>,
    #[serde(default)]
    pub dblp: DblpSourceConfig,
    #[serde(default)]
    pub arxiv: ArxivSourceConfig,
}

#[derive(Deserialize, Clone, Debug)]
pub struct DblpSourceConfig {
    #[serde(default = "default_dblp_base_url")]
    pub base_url: String,
    #[serde(default = "default_dblp_page_size")]
    pub page_size: usize,
    #[serde(default = "default_dblp_delay_ms")]
    pub delay_ms: u64,
    #[serde(default = "default_dblp_enable_cache")]
    pub enable_cache: bool,
}

impl Default for DblpSourceConfig {
    fn default() -> Self {
        Self {
            base_url: default_dblp_base_url(),
            page_size: default_dblp_page_size(),
            delay_ms: default_dblp_delay_ms(),
            enable_cache: default_dblp_enable_cache(),
        }
    }
}

fn default_dblp_base_url() -> String {
    "https://dblp.org/search/publ/api".to_string()
}
fn default_dblp_page_size() -> usize {
    1000
}
fn default_dblp_delay_ms() -> u64 {
    1000
}
fn default_dblp_enable_cache() -> bool {
    true
}

#[derive(Deserialize, Clone, Debug)]
pub struct ArxivSourceConfig {
    #[serde(default = "default_arxiv_base_url")]
    pub base_url: String,
    #[serde(default = "default_arxiv_page_size")]
    pub page_size: usize,
    #[serde(default = "default_arxiv_channel_size")]
    pub channel_size: usize,
    #[serde(default = "default_arxiv_delay_ms")]
    pub delay_ms: u64,
}

impl Default for ArxivSourceConfig {
    fn default() -> Self {
        Self {
            base_url: default_arxiv_base_url(),
            page_size: default_arxiv_page_size(),
            channel_size: default_arxiv_channel_size(),
            delay_ms: default_arxiv_delay_ms(),
        }
    }
}

fn default_arxiv_base_url() -> String {
    "http://export.arxiv.org/api/query".to_string()
}
fn default_arxiv_page_size() -> usize {
    100
}
fn default_arxiv_channel_size() -> usize {
    8
}
fn default_arxiv_delay_ms() -> u64 {
    200
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let content = fs::read_to_string("config.json")?;
    serde_json::from_str(&content).map_err(Into::into)
}
