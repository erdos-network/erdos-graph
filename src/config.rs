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
fn default_mega_paper_threshold() -> usize {
    50
}
fn default_max_edges_per_author() -> usize {
    5_000
}
fn default_max_authors_to_prefetch() -> usize {
    500
}
fn default_prefetch_skip_bloom_threshold() -> f64 {
    0.95
}
fn default_bloom_sample_size() -> usize {
    100
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
    /// Threshold for considering a paper a "mega-paper" (number of authors)
    #[serde(default = "default_mega_paper_threshold")]
    pub mega_paper_threshold: usize,
    /// Maximum edges to prefetch per author to bound query cost
    #[serde(default = "default_max_edges_per_author")]
    pub max_edges_per_author: usize,
    /// Maximum total authors to prefetch in a batch to avoid expensive prefetches
    #[serde(default = "default_max_authors_to_prefetch")]
    pub max_authors_to_prefetch: usize,
    /// Bloom hit rate threshold to skip prefetch for mega-papers (0.0-1.0)
    #[serde(default = "default_prefetch_skip_bloom_threshold")]
    pub prefetch_skip_bloom_threshold: f64,
    /// Number of edges to sample for estimating bloom coverage
    #[serde(default = "default_bloom_sample_size")]
    pub bloom_sample_size: usize,
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
            mega_paper_threshold: default_mega_paper_threshold(),
            max_edges_per_author: default_max_edges_per_author(),
            max_authors_to_prefetch: default_max_authors_to_prefetch(),
            prefetch_skip_bloom_threshold: default_prefetch_skip_bloom_threshold(),
            bloom_sample_size: default_bloom_sample_size(),
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
    #[serde(default)]
    pub zbmath: ZbmathSourceConfig,
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
    /// Frequency of longer pauses (every N queries)
    #[serde(default = "default_dblp_long_pause_frequency")]
    pub long_pause_frequency: usize,
    /// Duration of longer pause in milliseconds
    #[serde(default = "default_dblp_long_pause_ms")]
    pub long_pause_ms: u64,
    /// Base URL for XML dumps
    #[serde(default = "default_dblp_xml_base_url")]
    pub xml_base_url: String,
    /// Directory to temporarily store downloaded XML files
    #[serde(default = "default_dblp_xml_download_dir")]
    pub xml_download_dir: String,
    /// Maximum number of retries for 429 errors
    #[serde(default = "default_dblp_max_retries")]
    pub max_retries: u32,
    /// Delay in milliseconds before retrying after a 429 error
    #[serde(default = "default_dblp_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

impl Default for DblpSourceConfig {
    fn default() -> Self {
        Self {
            base_url: default_dblp_base_url(),
            page_size: default_dblp_page_size(),
            delay_ms: default_dblp_delay_ms(),
            enable_cache: default_dblp_enable_cache(),
            cache_dir: default_dblp_cache_dir(),
            long_pause_frequency: default_dblp_long_pause_frequency(),
            long_pause_ms: default_dblp_long_pause_ms(),
            xml_base_url: default_dblp_xml_base_url(),
            xml_download_dir: default_dblp_xml_download_dir(),
            max_retries: default_dblp_max_retries(),
            retry_delay_ms: default_dblp_retry_delay_ms(),
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
fn default_dblp_long_pause_frequency() -> usize {
    10
}
fn default_dblp_long_pause_ms() -> u64 {
    2000
}
fn default_dblp_xml_base_url() -> String {
    "https://dblp.org/xml".to_string()
}
fn default_dblp_xml_download_dir() -> String {
    ".dblp_xml_downloads".to_string()
}
fn default_dblp_max_retries() -> u32 {
    5
}
fn default_dblp_retry_delay_ms() -> u64 {
    10_000
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
    5000 // ArXiv supports up to 5000 results per request
}
fn default_arxiv_channel_size() -> usize {
    8
}
fn default_arxiv_delay_ms() -> u64 {
    100 // Reduced delay - ArXiv rate limit is 1 req/sec, we're well under that
}

#[derive(Deserialize, Clone, Debug)]
pub struct ZbmathSourceConfig {
    #[serde(default = "default_zbmath_base_url")]
    pub base_url: String,
    #[serde(default = "default_zbmath_delay_between_pages_ms")]
    pub delay_between_pages_ms: u64,
}

impl Default for ZbmathSourceConfig {
    fn default() -> Self {
        Self {
            base_url: default_zbmath_base_url(),
            delay_between_pages_ms: default_zbmath_delay_between_pages_ms(),
        }
    }
}

fn default_zbmath_base_url() -> String {
    "https://oai.zbmath.org/v1/".to_string()
}

fn default_zbmath_delay_between_pages_ms() -> u64 {
    100 // Faster pagination for OAI-PMH API
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let content = fs::read_to_string("config.json")?;
    serde_json::from_str(&content).map_err(Into::into)
}
