//! DBLP (Database systems and Logic Programming) Search API Scraper
//!
//! This module implements a scraper for the DBLP computer science bibliography database using its Search API.
//!
//! # Data Source
//! - API: https://dblp.org/search/publ/api
//! - Format: JSON
//! - Method: Paged search queries by year

use crate::db::ingestion::PublicationRecord;
use crate::scrapers::scraper::Scraper;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value; // Add this
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;

/// DBLP scraper that implements the Scraper trait.
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
    async fn scrape_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
        scrape_range_with_config(start, end, self.config.clone()).await
    }
}

/// Configuration for DBLP scraper
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

#[derive(Debug, Deserialize)]
struct DblpResponse {
    result: DblpResult,
}

#[derive(Debug, Deserialize)]
struct DblpResult {
    hits: DblpHits,
}

#[derive(Debug, Deserialize)]
struct DblpHits {
    #[serde(default)]
    hit: Vec<DblpHit>,
    #[serde(default)]
    _sent: Value, // Changed from String to Value, prefixed with _ to suppress warning
    #[serde(default)]
    total: Value, // Changed from String to Value
}

#[derive(Debug, Deserialize)]
struct DblpHit {
    info: Option<DblpInfo>,
}

#[derive(Debug, Deserialize)]
struct DblpInfo {
    title: Option<String>,
    authors: Option<DblpAuthors>,
    year: Option<String>,
    venue: Option<String>,
    key: Option<String>,
    // other fields like type, doi, url exist but we focus on these
}

#[derive(Debug, Deserialize)]
struct DblpAuthors {
    #[serde(default)]
    author: Vec<StringOrStruct>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringOrStruct {
    String(String),
    Struct { text: String },
}

// --- Implementation ---

/// Scrapes DBLP publication data for a specified date range using the Search API.
///
/// It iterates through each year in the range [start_date, end_date] and queries `year:YYYY`.
pub async fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    scrape_range_with_config(start_date, end_date, DblpConfig::default()).await
}

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

    println!(
        "Starting DBLP API scrape for years {} to {}...",
        start_year, end_year
    );

    for year in start_year..=end_year {
        println!("Fetching DBLP records for year {}...", year);
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

    println!(
        "DBLP scraping complete. Found {} records.",
        all_records.len()
    );
    Ok(all_records)
}

/// Helper function to fetch URL with file-based caching
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
