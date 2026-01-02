//! DBLP Search API Scraper
//!
//! This module implements a scraper for the DBLP computer science bibliography database using its Search API.
//!
//! # Data Source
//! - API: <https://dblp.org/search/publ/api>
//! - Format: JSON
//! - Method: Paged search queries by year
//!
//! # Implementation Details
//! The scraper works by iterating through each year in the requested date range and querying `year:YYYY`.
//! It handles pagination automatically and respects a configurable delay between requests to be polite to the API.
//!
//! # Example
//! ```rust,no_run
//! use erdos_graph::scrapers::dblp::DblpScraper;
//! use erdos_graph::scrapers::scraper::Scraper;
//! use chrono::{Utc, TimeZone};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let scraper = DblpScraper::new();
//! let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
//! let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();
//!
//! let records = scraper.scrape_range(start, end).await?;
//! println!("Found {} records", records.len());
//! # Ok(())
//! # }
//! ```

use crate::db::ingestion::PublicationRecord;
use crate::scrapers::scraper::Scraper;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;

/// DBLP scraper that implements the Scraper trait.
///
/// This struct wraps the configuration and provides the implementation for the
/// `Scraper` trait methods.
#[derive(Clone, Debug)]
pub struct DblpScraper {
    config: DblpConfig,
}

impl DblpScraper {
    /// Create a new DblpScraper with default configuration.
    pub fn new() -> Self {
        Self {
            config: DblpConfig::default(),
        }
    }

    /// Create a new DblpScraper with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - A `DblpConfig` instance containing the desired settings.
    pub fn with_config(config: DblpConfig) -> Self {
        Self { config }
    }
}

impl Default for DblpScraper {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Scraper for DblpScraper {
    /// Scrapes DBLP for publications within the given date range.
    ///
    /// This method delegates to `scrape_range_with_config` using the scraper's configuration.
    async fn scrape_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
        scrape_range_with_config(start, end, self.config.clone()).await
    }
}

/// Configuration for the DBLP scraper.
///
/// This struct holds configuration parameters that control how the scraper interacts
/// with the DBLP API, including the base URL, pagination size, and rate limiting.
///
/// # Fields
///
/// * `base_url`: The base URL for the DBLP Search API. Defaults to `https://dblp.org/search/publ/api`.
/// * `page_size`: The number of results to request per page. DBLP typically allows up to 1000.
/// * `delay_ms`: The delay in milliseconds between requests to avoid hitting rate limits.
///
/// # Environment Variables
///
/// The `Default` implementation looks for the following environment variables:
/// * `DBLP_BASE_URL`: Overrides the default base URL.
#[derive(Clone, Debug)]
pub struct DblpConfig {
    /// Base URL for the DBLP Search API
    pub base_url: String,
    /// Number of hits per page (max 1000 usually)
    pub page_size: usize,
    /// Delay between requests in milliseconds
    pub delay_ms: u64,
}

impl Default for DblpConfig {
    fn default() -> Self {
        let base_url = std::env::var("DBLP_BASE_URL")
            .unwrap_or_else(|_| "https://dblp.org/search/publ/api".to_string());
        Self {
            base_url,
            page_size: 1000,
            delay_ms: 1000, // Be polite
        }
    }
}

// --- DBLP API Response Structures ---

/// Top-level response structure from the DBLP Search API.
#[derive(Debug, Deserialize)]
struct DblpResponse {
    result: DblpResult,
}

/// Container for the search results.
#[derive(Debug, Deserialize)]
struct DblpResult {
    hits: DblpHits,
}

/// Contains the list of hits and metadata about the search result count.
#[derive(Debug, Deserialize)]
struct DblpHits {
    /// The list of publication hits.
    #[serde(default)]
    hit: Vec<DblpHit>,
    /// The number of results sent in this response.
    /// Note: Type is `Value` because DBLP can return this as a string or number.
    #[serde(default)]
    _sent: Value,
    /// The total number of matches for the query.
    /// Note: Type is `Value` because DBLP can return this as a string or number.
    #[serde(default)]
    total: Value,
}

/// Represents a single search hit (publication).
#[derive(Debug, Deserialize)]
struct DblpHit {
    /// The actual publication info.
    info: Option<DblpInfo>,
}

/// detailed information about a publication.
#[derive(Debug, Deserialize)]
struct DblpInfo {
    /// Title of the publication.
    title: Option<String>,
    /// Authors of the publication.
    authors: Option<DblpAuthors>,
    /// Year of publication (as a string).
    year: Option<String>,
    /// Venue or journal name.
    venue: Option<String>,
    /// DBLP key for the publication.
    key: Option<String>,
    // other fields like type, doi, url exist but we focus on these
}

/// Container for the list of authors.
#[derive(Debug, Deserialize)]
struct DblpAuthors {
    /// List of authors, which can be simple strings or objects.
    #[serde(default)]
    author: Vec<StringOrStruct>,
}

/// Enum to handle DBLP's inconsistent author formatting.
///
/// Sometimes an author is just a string name, other times it's an object with a `text` field
/// (and potentially other fields like `pid`).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringOrStruct {
    String(String),
    Struct { text: String },
}

/// Scrapes DBLP publication data for a specified date range using the Search API.
///
/// This function iterates through each year in the provided range `[start_date, end_date]`
/// and performs a search query for `year:YYYY`. It handles pagination to retrieve all
/// results for each year.
///
/// # Arguments
///
/// * `start_date` - The start of the date range (inclusive).
/// * `end_date` - The end of the date range (inclusive).
///
/// # Returns
///
/// Returns a `Result` containing a vector of `PublicationRecord` on success, or a
/// `Box<dyn std::error::Error>` if an error occurs during scraping or parsing.
pub async fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    scrape_range_with_config(start_date, end_date, DblpConfig::default()).await
}

/// Scrapes DBLP publication data with a custom configuration.
///
/// This function contains the core logic for scraping. It:
/// 1. Iterates through each year in the date range.
/// 2. Constructs the DBLP API URL for the query `year:YYYY`.
/// 3. Fetches pages of results using the configured `page_size`.
/// 4. Caches responses to the `.dblp_cache` directory to avoid re-fetching.
/// 5. Parses the JSON response and converts hits to `PublicationRecord` objects.
/// 6. Respects the `delay_ms` configuration to rate limit requests.
///
/// # Arguments
///
/// * `start_date` - The start of the date range (inclusive).
/// * `end_date` - The end of the date range (inclusive).
/// * `config` - The `DblpConfig` to use for the scraper.
///
/// # Returns
///
/// Returns a `Result` containing a vector of `PublicationRecord` on success, or a
/// `Box<dyn std::error::Error>` if an error occurs.
pub async fn scrape_range_with_config(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    config: DblpConfig,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    if start_date >= end_date {
        return Ok(Vec::new());
    }

    let start_year = start_date.year();
    let end_year = end_date.year();
    let client = Client::new();
    let mut all_records = Vec::new();

    for year in start_year..=end_year {
        let mut first = 0;
        let query = format!("year:{}", year);

        loop {
            let url = format!(
                "{}?q={}&h={}&f={}&format=json",
                config.base_url, query, config.page_size, first
            );

            // Use cached fetch if available
            let body_text = match fetch_url_cached(&client, &url).await {
                Ok(text) => text,
                Err(e) => {
                    eprintln!("Failed to fetch URL {}: {}", url, e);
                    break;
                }
            };

            // DBLP sometimes returns malformed JSON or unexpected structures?
            // Parsing
            let dblp_resp: DblpResponse = match serde_json::from_str(&body_text) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("Failed to parse DBLP JSON: {}", e);
                    break;
                }
            };

            let hits = dblp_resp.result.hits.hit;
            let hits_len = hits.len();
            let total: usize = match &dblp_resp.result.hits.total {
                Value::String(s) => s.parse().unwrap_or(0),
                Value::Number(n) => n.as_u64().unwrap_or(0) as usize,
                _ => 0,
            };

            // Process hits
            for hit in hits {
                if let Some(record) = hit.info.and_then(convert_hit_to_record) {
                    all_records.push(record);
                }
            }

            // Pagination logic
            first += hits_len;
            if first >= total || hits_len == 0 {
                break;
            }

            // Rate limiting
            sleep(Duration::from_millis(config.delay_ms)).await;
        }
    }

    Ok(all_records)
}

/// Helper function to fetch URL with file-based caching.
///
/// This function:
/// 1. Computes a SHA256 hash of the URL to use as the cache filename.
/// 2. Checks if the file exists in `.dblp_cache`.
/// 3. If it exists, returns the content from the file.
/// 4. If not, fetches the URL using `reqwest`.
/// 5. If the fetch is successful, writes the content to the cache file and returns it.
async fn fetch_url_cached(
    client: &Client,
    url: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let cache_dir = Path::new(".dblp_cache");
    if !cache_dir.exists() {
        fs::create_dir_all(cache_dir)?;
    }

    let hash = format!("{:x}", Sha256::digest(url.as_bytes()));
    let cache_path = cache_dir.join(format!("{}.json", hash));

    if cache_path.exists() {
        // println!("Cache hit for URL: {}", url); // Optional logging
        let content = fs::read_to_string(&cache_path)?;
        return Ok(content);
    }

    // println!("Fetching URL: {}", url); // Optional logging
    let resp = client.get(url).send().await?;

    if !resp.status().is_success() {
        return Err(format!("HTTP error: {}", resp.status()).into());
    }

    let text = resp.text().await?;

    // Only cache successful responses
    fs::write(&cache_path, &text)?;

    Ok(text)
}

/// Converts a DBLP hit info into a `PublicationRecord`.
///
/// Returns `None` if required fields (title, authors) are missing or empty.
fn convert_hit_to_record(info: DblpInfo) -> Option<PublicationRecord> {
    let title = info.title?;
    let year_str = info.year?;
    let year: u32 = year_str.parse().ok()?;
    let key = info.key.unwrap_or_else(|| "unknown".to_string());

    // Extract authors
    let mut authors = Vec::new();
    if let Some(auths) = info.authors {
        for a in auths.author {
            let name = match a {
                StringOrStruct::String(s) => s,
                StringOrStruct::Struct { text } => text,
            };
            authors.push(name);
        }
    }

    // Check if empty required fields
    if title.trim().is_empty() || authors.is_empty() {
        return None;
    }

    Some(PublicationRecord {
        id: key,
        title,
        authors,
        year,
        venue: info.venue,
        source: "dblp".to_string(),
    })
}
