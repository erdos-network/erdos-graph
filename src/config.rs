#![coverage(off)]

use crate::logger::LogLevel;
use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Clone, Debug)]
pub struct Config {
    pub scrapers: ScraperConfig,
    pub ingestion: IngestionConfig,
    pub deduplication: DeduplicationConfig,
    #[serde(default)]
    pub edge_cache: EdgeCacheConfig,
    pub heartbeat_timeout_s: u64,
    pub polling_interval_ms: u64,
    #[serde(default = "default_log_level")]
    pub log_level: LogLevel,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            scrapers: ScraperConfig::default(),
            ingestion: IngestionConfig::default(),
            deduplication: DeduplicationConfig::default(),
            edge_cache: EdgeCacheConfig::default(),
            heartbeat_timeout_s: 30,
            polling_interval_ms: 100,
            log_level: default_log_level(),
        }
    }
}

fn default_log_level() -> LogLevel {
    LogLevel::Info
}

#[derive(Deserialize, Clone, Debug)]
pub struct DeduplicationConfig {
    pub title_similarity_threshold: f64,
    pub author_similarity_threshold: f64,
    #[serde(default = "default_bloom_filter_size")]
    pub bloom_filter_size: usize,
}

impl Default for DeduplicationConfig {
    fn default() -> Self {
        Self {
            title_similarity_threshold: 0.9,
            author_similarity_threshold: 0.5,
            bloom_filter_size: default_bloom_filter_size(),
        }
    }
}

fn default_bloom_filter_size() -> usize {
    200_000
}

#[derive(Deserialize, Clone, Debug)]
pub struct EdgeCacheConfig {
    #[serde(default = "default_edge_hot_size")]
    pub hot_size: usize,
    #[serde(default = "default_edge_warm_size")]
    pub warm_size: usize,
    #[serde(default = "default_edge_bloom_size")]
    pub bloom_size: usize,
}

impl Default for EdgeCacheConfig {
    fn default() -> Self {
        Self {
            hot_size: default_edge_hot_size(),
            warm_size: default_edge_warm_size(),
            bloom_size: default_edge_bloom_size(),
        }
    }
}

fn default_edge_hot_size() -> usize {
    1_000_000
}
fn default_edge_warm_size() -> usize {
    5_000_000
}
fn default_edge_bloom_size() -> usize {
    100_000_000
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
    /// Write buffer initial capacity
    #[serde(default = "default_write_buffer_capacity")]
    pub write_buffer_capacity: usize,
    /// Estimated edges per paper for bloom filter sizing
    #[serde(default = "default_estimated_edges_per_paper")]
    pub estimated_edges_per_paper: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            chunk_size_days: 1,
            initial_start_date: "1932-01-01T00:00:00Z".to_string(),
            weekly_days: 7,
            checkpoint_dir: None,
            write_buffer_capacity: default_write_buffer_capacity(),
            estimated_edges_per_paper: default_estimated_edges_per_paper(),
        }
    }
}

fn default_write_buffer_capacity() -> usize {
    5000
}

fn default_estimated_edges_per_paper() -> usize {
    50
}

#[derive(Deserialize, Clone, Debug, Default)]
pub struct ScraperConfig {
    #[serde(default)]
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
    #[serde(default = "default_dblp_cache_dir")]
    pub cache_dir: String,
}

impl Default for DblpSourceConfig {
    fn default() -> Self {
        Self {
            base_url: default_dblp_base_url(),
            page_size: default_dblp_page_size(),
            delay_ms: default_dblp_delay_ms(),
            enable_cache: default_dblp_enable_cache(),
            cache_dir: default_dblp_cache_dir(),
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
fn default_dblp_cache_dir() -> String {
    ".dblp_cache".to_string()
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
