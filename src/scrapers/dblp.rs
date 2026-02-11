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
//! use erdos_graph::utilities::thread_safe_queue::{ThreadSafeQueue, QueueConfig};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let scraper = DblpScraper::new();
//! let start = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
//! let end = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 59).unwrap();
//!
//! let queue = ThreadSafeQueue::new(QueueConfig::default());
//! let producer = queue.create_producer();
//! scraper.scrape_range(start, end, producer).await?;
//! println!("Scraping completed");
//! # Ok(())
//! # }
//! ```

use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::scraper::Scraper;
use crate::utilities::thread_safe_queue::QueueProducer;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Utc};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::BufReader;
use std::path::Path;
use std::time::Duration;
use tokio::time::sleep;
use flate2::read::GzDecoder;
use quick_xml::events::Event;
use quick_xml::Reader;
use quick_xml::escape::unescape as quick_unescape;

use crate::config::DblpSourceConfig;

/// Scraping mode for DBLP
#[derive(Clone, Debug, PartialEq)]
pub enum DblpMode {
    /// Use the DBLP Search API with multiple queries
    Search,
    /// Use XML dump files (monthly snapshots)
    Xml,
}

impl DblpMode {
    /// Parse mode from string
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "search" => Ok(DblpMode::Search),
            "xml" => Ok(DblpMode::Xml),
            _ => Err(format!("Invalid DBLP mode: '{}'. Must be 'search' or 'xml'", s)),
        }
    }
}

/// Dblp scraper that implements the Scraper trait.
///
/// This struct wraps the configuration and provides the implementation for the
/// `Scraper` trait methods.
#[derive(Clone, Debug)]
pub struct DblpScraper {
    config: DblpSourceConfig,
}

impl DblpScraper {
    /// Create a new DblpScraper with default configuration.
    pub fn new() -> Self {
        Self {
            config: DblpSourceConfig::default(),
        }
    }

    /// Create a new DblpScraper with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - A `DblpSourceConfig` instance containing the desired settings.
    pub fn with_config(config: DblpSourceConfig) -> Self {
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
    /// By default, uses the Search API mode.
    async fn scrape_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        producer: QueueProducer<PublicationRecord>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        scrape_range_with_config(start, end, self.config.clone(), producer).await
    }

    /// Scrapes DBLP with a specific mode.
    ///
    /// # Arguments
    ///
    /// * `start` - The start of the date range (inclusive).
    /// * `end` - The end of the date range (inclusive).
    /// * `mode` - The scraping mode: "search" for API queries, "xml" for XML dumps.
    /// * `producer` - Queue producer to submit parsed records.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or failure.
    async fn scrape_range_with_mode(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        mode: &str,
        producer: QueueProducer<PublicationRecord>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dblp_mode = DblpMode::from_str(mode)?;
        scrape_range_with_mode(start, end, dblp_mode, self.config.clone(), producer).await
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

/// Enum to handle fields that can be a single string or a list of strings (e.g. venue, title).
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum StringOrSeq {
    String(String),
    Seq(Vec<String>),
}

/// detailed information about a publication.
#[derive(Debug, Deserialize)]
struct DblpInfo {
    /// Title of the publication.
    title: Option<StringOrSeq>,
    /// Authors of the publication.
    authors: Option<DblpAuthors>,
    /// Year of publication (as a string).
    year: Option<String>,
    /// Venue or journal name.
    venue: Option<StringOrSeq>,
    /// DBLP key for the publication.
    key: Option<String>,
    // other fields like type, doi, url exist but we focus on these
}

/// Container for the list of authors.
#[derive(Debug, Deserialize)]
struct DblpAuthors {
    /// List of authors, which can be simple strings or objects, or a single author.
    /// DBLP returns a single object when there's one author, and an array for multiple.
    #[serde(default)]
    author: AuthorField,
}

/// Enum to handle DBLP's inconsistent author field format.
///
/// DBLP returns a single author object for single-author publications,
/// and an array for multi-author publications.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AuthorField {
    Single(StringOrStruct),
    Multiple(Vec<StringOrStruct>),
}

impl Default for AuthorField {
    fn default() -> Self {
        AuthorField::Multiple(Vec::new())
    }
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
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    scrape_range_with_config(start_date, end_date, DblpSourceConfig::default(), producer).await
}

/// Scrapes DBLP publication data with a custom configuration.
///
/// This function contains the core logic for scraping. It:
/// - Iterate through each year in the date range
/// - Construct the DBLP API URL for the query
/// - Fetch pages of results using the configured page size
/// - Cache responses to avoid re-fetching
/// - Parse JSON response and convert hits to records
/// - Rate limit requests based on configuration
///
/// # Arguments
///
/// * `start_date` - The start of the date range (inclusive).
/// * `end_date` - The end of the date range (inclusive).
/// * `config` - The `DblpSourceConfig` to use for the scraper.
///
/// # Returns
///
/// Returns a `Result` containing a vector of `PublicationRecord` on success, or a
/// `Box<dyn std::error::Error>` if an error occurs.
pub async fn scrape_range_with_config(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    config: DblpSourceConfig,
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    if start_date >= end_date {
        return Ok(());
    }

    let start_year = start_date.year();
    let end_year = end_date.year();
    let client = Client::new();
    let current_year = Utc::now().year();

    for year in start_year..=end_year {
        // For historic years, only scrape if the range includes the start of the year
        // We consider "active" years to be the current and previous year.
        let is_active_year = year >= current_year - 1;

        if !is_active_year {
            // Safe to unwrap as 1/1 is always valid
            let jan1 = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc();

            // Check if jan1 is within [start_date, end_date)
            if !(start_date <= jan1 && jan1 < end_date) {
                logger::debug(&format!(
                    "Skipping DBLP scrape for historic year {} as range {} to {} does not include Jan 1",
                    year, start_date, end_date
                ));
                continue;
            }
        }

        // Split year into multiple disjoint queries to work around DBLP's 10K result limit
        let mut query_filters = Vec::new();

        // Split inproceedings by venue prefixes
        let conf_prefixes = vec![
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        ];
        for prefix in conf_prefixes {
            query_filters.push(format!(
                "year:{} type:inproceedings venue:{}*",
                year, prefix
            ));
        }

        // Split articles by journal prefixes
        let journal_prefixes = vec![
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        ];
        for prefix in journal_prefixes {
            query_filters.push(format!("year:{} type:article venue:{}*", year, prefix));
        }

        // Books - split by first letter of title
        for prefix in vec![
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q",
            "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
        ] {
            query_filters.push(format!("year:{} type:book {}*", year, prefix));
        }

        // Other types with lower volume
        query_filters.extend(vec![
            format!("year:{} type:incollection", year),
            format!("year:{} type:phdthesis", year),
            format!("year:{} type:mastersthesis", year),
            format!("year:{} type:www", year),
            format!("year:{} type:data", year),
            format!("year:{} type:informal", year),
            format!("year:{} type:proceedings", year),
        ]);

        for (query_idx, query) in query_filters.iter().enumerate() {
            let mut first = 0;
            const MAX_RESULTS_PER_QUERY: usize = 10_000; // DBLP API limit

            loop {
                let url = format!(
                    "{}?q={}&h={}&f={}&format=json",
                    config.base_url, query, config.page_size, first
                );

                logger::debug(&format!("Fetching DBLP URL: {}", url));

                let use_cache = config.enable_cache && !is_active_year;
                let body_text =
                    match fetch_url_cached(&client, &url, use_cache, &config.cache_dir).await {
                        Ok(text) => text,
                        Err(e) => {
                            logger::error(&format!("Failed to fetch URL {}: {}", url, e));
                            break;
                        }
                    };

                let dblp_resp: DblpResponse = match serde_json::from_str(&body_text) {
                    Ok(v) => v,
                    Err(e) => {
                        logger::error(&format!("Failed to parse DBLP JSON: {}", e));
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

                for hit in hits {
                    #[allow(clippy::collapsible_if)]
                    if let Some(record) = hit.info.and_then(convert_hit_to_record) {
                        if let Err(e) = producer.submit(record) {
                            logger::error(&format!("Failed to submit record: {}", e));
                        }
                    }
                }

                first += hits_len;

                if hits_len == 0 || first >= MAX_RESULTS_PER_QUERY {
                    break;
                }

                if total > 0 && first >= total {
                    break;
                }

                sleep(Duration::from_millis(config.delay_ms)).await;
            }

            // Rate limit between queries, with longer pauses periodically to avoid 429 errors
            if (query_idx + 1) % config.long_pause_frequency == 0 {
                sleep(Duration::from_millis(config.long_pause_ms)).await;
            } else {
                sleep(Duration::from_millis(config.delay_ms)).await;
            }
        }
    }

    Ok(())
}

/// Helper function to fetch URL with file-based caching.
///
/// This function:
/// - Compute SHA256 hash of the URL to use as the cache filename
/// - Check if the file exists in the cache directory
/// - Return content from file if it exists
/// - Fetch URL using reqwest if not cached
/// - Write content to cache file and return it on success
async fn fetch_url_cached(
    client: &Client,
    url: &str,
    enable_cache: bool,
    cache_dir_str: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    if !enable_cache {
        let resp = client.get(url).send().await?;
        if !resp.status().is_success() {
            return Err(format!("HTTP error: {}", resp.status()).into());
        }
        return Ok(resp.text().await?);
    }

    let cache_dir = Path::new(cache_dir_str);
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
    let title = match info.title? {
        StringOrSeq::String(s) => s,
        StringOrSeq::Seq(list) => list.join(" "),
    };

    let year_str = info.year?;
    let year: u32 = year_str.parse().ok()?;
    let key = info.key.unwrap_or_else(|| "unknown".to_string());

    // Extract authors
    let mut authors = Vec::new();
    if let Some(auths) = info.authors {
        let author_list = match auths.author {
            AuthorField::Single(a) => vec![a],
            AuthorField::Multiple(list) => list,
        };
        for a in author_list {
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

    let venue = info.venue.map(|v| match v {
        StringOrSeq::String(s) => s,
        StringOrSeq::Seq(list) => list.join(", "),
    });

    Some(PublicationRecord {
        id: key,
        title,
        authors,
        year,
        venue,
        source: "dblp".to_string(),
    })
}

// --- XML Dump Scraping Functions ---

/// Scrapes DBLP using XML dump files for a specified date range.
///
/// This function downloads monthly XML snapshots, parses them efficiently,
/// and deletes each file after processing to save disk space.
///
/// # Arguments
///
/// * `start_date` - The start of the date range (inclusive).
/// * `end_date` - The end of the date range (inclusive).
/// * `config` - The `DblpSourceConfig` to use for the scraper.
/// * `producer` - Queue producer to submit parsed records.
///
/// # Returns
///
/// Returns a `Result` indicating success or failure.
pub async fn scrape_range_xml(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    config: DblpSourceConfig,
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    if start_date >= end_date {
        return Ok(());
    }

    let start_year = start_date.year();
    let end_year = end_date.year();
    let start_month = start_date.month();
    let end_month = end_date.month();

    // Create download directory if it doesn't exist
    let download_dir = Path::new(&config.xml_download_dir);
    if !download_dir.exists() {
        fs::create_dir_all(download_dir)?;
    }

    let client = Client::new();

    // Generate list of monthly snapshots to download
    let mut snapshots = Vec::new();
    
    for year in start_year..=end_year {
        let month_start = if year == start_year { start_month } else { 1 };
        let month_end = if year == end_year { end_month } else { 12 };
        
        for month in month_start..=month_end {
            snapshots.push((year, month));
        }
    }

    logger::info(&format!(
        "XML mode: Processing {} monthly snapshots from {}-{:02} to {}-{:02}",
        snapshots.len(),
        start_year,
        start_month,
        end_year,
        end_month
    ));

    // Process each monthly snapshot
    for (year, month) in snapshots {
        let url = format!(
            "{}/release/dblp-{:04}-{:02}-01.xml.gz",
            config.xml_base_url, year, month
        );
        
        let filename = format!("dblp-{:04}-{:02}-01.xml.gz", year, month);
        let filepath = download_dir.join(&filename);

        logger::info(&format!("Downloading XML snapshot: {}", url));

        // Download the file
        match download_file(&client, &url, &filepath).await {
            Ok(_) => {
                logger::info(&format!("Successfully downloaded: {}", filename));
                
                // Parse the XML file
                logger::info(&format!("Parsing XML file: {}", filename));
                match parse_xml_dump(&filepath, start_date, end_date, &producer) {
                    Ok(count) => {
                        logger::info(&format!(
                            "Parsed {} records from {}",
                            count, filename
                        ));
                    }
                    Err(e) => {
                        logger::error(&format!("Failed to parse {}: {}", filename, e));
                    }
                }

                // Delete the file to save space
                if let Err(e) = fs::remove_file(&filepath) {
                    logger::warn(&format!("Failed to delete {}: {}", filename, e));
                } else {
                    logger::debug(&format!("Deleted temporary file: {}", filename));
                }
            }
            Err(e) => {
                logger::error(&format!("Failed to download {}: {}", url, e));
                // Continue with next snapshot instead of failing completely
                continue;
            }
        }
    }

    Ok(())
}

/// Downloads a file from a URL to a local path.
async fn download_file(
    client: &Client,
    url: &str,
    filepath: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = client.get(url).send().await?;
    
    if !response.status().is_success() {
        return Err(format!("HTTP error: {}", response.status()).into());
    }

    let bytes = response.bytes().await?;
    fs::write(filepath, bytes)?;
    
    Ok(())
}

/// Parses a gzipped DBLP XML dump file and extracts publication records.
///
/// This function uses streaming XML parsing to handle large files efficiently.
/// It filters records to only include those within the specified date range.
///
/// # Arguments
///
/// * `filepath` - Path to the gzipped XML file
/// * `start_date` - Start of date range filter
/// * `end_date` - End of date range filter
/// * `producer` - Queue producer to submit parsed records
///
/// # Returns
///
/// Returns the number of records parsed, or an error.
fn parse_xml_dump(
    filepath: &Path,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    producer: &QueueProducer<PublicationRecord>,
) -> Result<usize, Box<dyn std::error::Error>> {
    let file = fs::File::open(filepath)?;
    let buf_reader = BufReader::new(file);
    let gz_decoder = GzDecoder::new(buf_reader);
    let buf_gz_reader = BufReader::new(gz_decoder);
    let mut reader = Reader::from_reader(buf_gz_reader);
    reader.config_mut().trim_text(true);

    let mut count = 0;
    let mut buf = Vec::new();
    
    // Current publication being parsed
    let mut current_pub: Option<XmlPublication> = None;
    let mut current_text = String::new();
    let mut in_author = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = e.name();
                match name.as_ref() {
                    b"article" | b"inproceedings" | b"proceedings" | b"book" 
                    | b"incollection" | b"phdthesis" | b"mastersthesis" => {
                        // Start of a new publication
                        let key = e.attributes()
                            .filter_map(|a| a.ok())
                            .find(|a| a.key.as_ref() == b"key")
                            .and_then(|a| String::from_utf8(a.value.to_vec()).ok());
                        
                        current_pub = Some(XmlPublication {
                            key: key.unwrap_or_else(|| "unknown".to_string()),
                            title: None,
                            authors: Vec::new(),
                            year: None,
                            venue: None,
                        });
                    }
                    b"author" => {
                        in_author = true;
                        current_text.clear();
                    }
                    b"title" | b"year" | b"journal" | b"booktitle" => {
                        current_text.clear();
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(e)) => {
                // Decode HTML entities like &amp; to &
                let text = std::str::from_utf8(e.as_ref()).unwrap_or("");
                if let Ok(unescaped) = quick_unescape(text) {
                    current_text.push_str(&unescaped);
                }
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                match name.as_ref() {
                    b"article" | b"inproceedings" | b"proceedings" | b"book" 
                    | b"incollection" | b"phdthesis" | b"mastersthesis" => {
                        // End of publication - convert and submit if valid
                        if let Some(pub_data) = current_pub.take() {
                            if let Some(record) = convert_xml_to_record(pub_data, start_date, end_date) {
                                if let Err(e) = producer.submit(record) {
                                    logger::error(&format!("Failed to submit record: {}", e));
                                } else {
                                    count += 1;
                                }
                            }
                        }
                    }
                    b"author" => {
                        if in_author {
                            if let Some(ref mut pub_data) = current_pub {
                                pub_data.authors.push(current_text.trim().to_string());
                            }
                            in_author = false;
                            current_text.clear();
                        }
                    }
                    b"title" => {
                        if let Some(ref mut pub_data) = current_pub {
                            pub_data.title = Some(current_text.trim().to_string());
                        }
                        current_text.clear();
                    }
                    b"year" => {
                        if let Some(ref mut pub_data) = current_pub {
                            pub_data.year = current_text.trim().parse().ok();
                        }
                        current_text.clear();
                    }
                    b"journal" | b"booktitle" => {
                        if let Some(ref mut pub_data) = current_pub {
                            if pub_data.venue.is_none() {
                                pub_data.venue = Some(current_text.trim().to_string());
                            }
                        }
                        current_text.clear();
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                logger::warn(&format!("XML parse error at position {}: {}", reader.buffer_position(), e));
                // Continue parsing despite errors
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(count)
}

/// Intermediate structure for parsing XML publications
#[derive(Debug)]
struct XmlPublication {
    key: String,
    title: Option<String>,
    authors: Vec<String>,
    year: Option<u32>,
    venue: Option<String>,
}

/// Converts an XML publication to a PublicationRecord.
///
/// Returns `None` if the record doesn't meet requirements (missing fields,
/// outside date range, etc.).
fn convert_xml_to_record(
    pub_data: XmlPublication,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Option<PublicationRecord> {
    // Check required fields
    let title = pub_data.title?;
    let year = pub_data.year?;
    
    if title.trim().is_empty() || pub_data.authors.is_empty() {
        return None;
    }

    // Filter by date range
    let start_year = start_date.year() as u32;
    let end_year = end_date.year() as u32;
    
    if year < start_year || year > end_year {
        return None;
    }

    Some(PublicationRecord {
        id: pub_data.key,
        title,
        authors: pub_data.authors,
        year,
        venue: pub_data.venue,
        source: "dblp".to_string(),
    })
}

/// Scrapes DBLP with the specified mode (Search API or XML dump).
///
/// This is a convenience function that dispatches to the appropriate
/// implementation based on the mode.
///
/// # Arguments
///
/// * `start_date` - The start of the date range (inclusive).
/// * `end_date` - The end of the date range (inclusive).
/// * `mode` - The scraping mode (Search or Xml).
/// * `config` - The `DblpSourceConfig` to use for the scraper.
/// * `producer` - Queue producer to submit parsed records.
///
/// # Returns
///
/// Returns a `Result` indicating success or failure.
pub async fn scrape_range_with_mode(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    mode: DblpMode,
    config: DblpSourceConfig,
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    match mode {
        DblpMode::Search => scrape_range_with_config(start_date, end_date, config, producer).await,
        DblpMode::Xml => scrape_range_xml(start_date, end_date, config, producer).await,
    }
}
