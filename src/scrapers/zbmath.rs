use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::scraper::Scraper;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::time::Duration as StdDuration;
use tokio::time::sleep;

const ZBMATH_BASE_URL: &str = "https://oai.zbmath.org/v1/";

/// ZbMATH scraper that implements the Scraper trait.
#[derive(Clone, Debug)]
pub struct ZbmathScraper {
    config: ZbmathConfig,
}

impl ZbmathScraper {
    /// Create a new ZbmathScraper with default configuration.
    pub fn new() -> Self {
        Self {
            config: ZbmathConfig::default(),
        }
    }

    /// Create a new ZbmathScraper with custom configuration.
    pub fn with_config(config: ZbmathConfig) -> Self {
        Self { config }
    }
}

impl Default for ZbmathScraper {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Scraper for ZbmathScraper {
    async fn scrape_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
        scrape_range_with_config(start, end, self.config.clone()).await
    }
}

#[derive(Clone, Debug)]
pub struct ZbmathConfig {
    pub base_url: String,
    pub delay_between_pages_ms: u64,
}

impl Default for ZbmathConfig {
    fn default() -> Self {
        Self {
            base_url: ZBMATH_BASE_URL.to_string(),
            delay_between_pages_ms: 500,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct OaiPmh {
    #[serde(rename = "ListRecords")]
    pub(crate) list_records: Option<ListRecords>,
    #[serde(rename = "error")]
    pub(crate) error: Option<OaiError>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListRecords {
    #[serde(rename = "record", default)]
    pub(crate) records: Vec<Record>,
    #[serde(rename = "resumptionToken")]
    pub(crate) resumption_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Record {
    pub(crate) header: RecordHeader,
    pub(crate) metadata: Option<Metadata>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(crate) struct RecordHeader {
    pub(crate) identifier: String,
    #[serde(rename = "datestamp")]
    pub(crate) date_stamp: String,
    #[serde(rename = "setSpec", default)]
    pub(crate) set_spec: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Metadata {
    #[serde(rename = "dc")]
    pub(crate) dc: DublinCore,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub(crate) struct DublinCore {
    #[serde(rename = "contributor")]
    pub(crate) contributor: Option<String>,
    #[serde(rename = "creator")]
    pub(crate) creator: Option<String>,
    #[serde(rename = "date")]
    pub(crate) date: Option<String>,
    #[serde(rename = "identifier")]
    pub(crate) identifier: Option<String>,
    #[serde(rename = "language")]
    pub(crate) language: Option<String>,
    #[serde(rename = "publisher")]
    pub(crate) publisher: Option<String>,
    #[serde(rename = "relation")]
    pub(crate) relation: Option<String>,
    #[serde(rename = "rights")]
    pub(crate) rights: Option<String>,
    #[serde(rename = "source")]
    pub(crate) source: Option<String>,
    #[serde(rename = "subject")]
    pub(crate) subject: Option<String>,
    #[serde(rename = "title")]
    pub(crate) title: Option<String>,
    #[serde(rename = "type")]
    pub(crate) doc_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct OaiError {
    #[serde(rename = "code")]
    pub(crate) code: String,
    #[serde(rename = "$value")]
    pub(crate) message: String,
}

/// Scrapes publication records from zbMATH for a given date range.
///
/// This function handles pagination, rate limiting, and error responses.
/// Date range chunking is handled by the ingestion layer, so this function
/// scrapes the entire provided date range.
///
/// # Implementation Details
/// - Makes requests to the zbMATH Open OAI-PMH API (<https://oai.zbmath.org/v1/>)
/// - Handles pagination using resumption tokens (max 100 records per request)
/// - Includes a delay between paginated requests to be respectful to the API
/// - Parses XML responses using Dublin Core metadata format
/// - Converts parsed data into `PublicationRecord` structs
///
/// # Arguments
/// * `start_date` - The beginning of the date range (inclusive)
/// * `end_date` - The end of the date range (exclusive)
///
/// # Returns
/// A vector of `PublicationRecord` objects representing publications from zbMATH
///
/// # Errors
/// Returns an error if:
/// - Network requests fail
/// - API returns error responses that can't be handled
/// - XML parsing fails
/// - Date formatting fails
///
/// # Example
/// ```ignore
/// use chrono::{TimeZone, Utc};
///
/// let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
/// let end = Utc.with_ymd_and_hms(2024, 1, 8, 0, 0, 0).unwrap();
/// let records = scrape_range(start, end).await?;
/// println!("Found {} publications", records.len());
/// ```
pub async fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    scrape_range_with_config(start_date, end_date, ZbmathConfig::default()).await
}

/// Scrapes publication records from zbMATH with custom configuration.
pub async fn scrape_range_with_config(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    config: ZbmathConfig,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    scrape_chunk_with_config(&client, start_date, end_date, &config).await
}

/// Scrapes a date range with custom configuration.
///
/// This function handles pagination for the given date range and processes
/// all pages of results. Date range chunking is handled by the ingestion layer.
pub async fn scrape_chunk_with_config(
    client: &reqwest::Client,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    config: &ZbmathConfig,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let mut all_records = Vec::new();
    let mut resumption_token: Option<String> = None;

    loop {
        let mut params = vec![
            ("verb", "ListRecords".to_string()),
            ("metadataPrefix", "oai_dc".to_string()),
        ];

        if let Some(token) = &resumption_token {
            // Use resumption token for pagination
            params.push(("resumptionToken", token.clone()));
        } else {
            // First request - use date range
            params.push(("from", start_date.format("%Y-%m-%dT%H:%M:%SZ").to_string()));
            params.push(("until", end_date.format("%Y-%m-%dT%H:%M:%SZ").to_string()));
        }

        logger::debug(&format!("Fetching zbMATH records: {:?}", params));

        let response = client
            .get(&config.base_url)
            .query(&params)
            .header("accept", "text/xml")
            .send()
            .await?;

        let status = response.status();
        let xml_text = response.text().await?;

        if !status.is_success() {
            // Try to parse as error response
            if let Ok(oai_response) = serde_xml_rs::from_str::<OaiPmh>(&xml_text) {
                if let Some(error) = oai_response.error {
                    match error.code.as_str() {
                        "noRecordsMatch" => {
                            // No records for this date range - that's okay
                            logger::info(&format!(
                                "No records found for date range {} to {}",
                                start_date.format("%Y-%m-%d"),
                                end_date.format("%Y-%m-%d")
                            ));
                            break;
                        }
                        "badArgument" => {
                            return Err(format!("Bad API argument: {}", error.message).into());
                        }
                        _ => {
                            return Err(
                                format!("API error {}: {}", error.code, error.message).into()
                            );
                        }
                    }
                }
            } else {
                return Err(format!("HTTP error {}: {}", status, xml_text).into());
            }
        }

        // Parse successful response
        let oai_response: OaiPmh = serde_xml_rs::from_str(&xml_text)
            .map_err(|e| format!("Failed to parse XML response: {}", e))?;

        if let Some(error) = oai_response.error {
            match error.code.as_str() {
                "noRecordsMatch" => {
                    // No records for this date range - that's okay
                    break;
                }
                _ => {
                    return Err(format!("API error {}: {}", error.code, error.message).into());
                }
            }
        }

        if let Some(list_records) = oai_response.list_records {
            logger::debug(&format!(
                "Found {} records in this chunk",
                list_records.records.len()
            ));
            // Convert OAI-PMH records to PublicationRecord
            for record in list_records.records {
                if let Some(publication) = convert_to_publication_record(record)? {
                    all_records.push(publication);
                }
            }

            // Check for pagination
            resumption_token = list_records.resumption_token;
            if resumption_token.is_none() || resumption_token.as_ref().unwrap().is_empty() {
                // No more pages
                break;
            }

            // Small delay between paginated requests
            sleep(StdDuration::from_millis(config.delay_between_pages_ms)).await;
        } else {
            // No list_records element - we're done
            break;
        }
    }

    Ok(all_records)
}

/// Converts an OAI-PMH record to a PublicationRecord.
pub(crate) fn convert_to_publication_record(
    record: Record,
) -> Result<Option<PublicationRecord>, Box<dyn std::error::Error>> {
    let metadata = match record.metadata {
        Some(m) => m,
        None => return Ok(None), // Skip records without metadata
    };

    let dc = metadata.dc;

    // Extract required fields
    let title = dc.title.unwrap_or_else(|| "Unknown Title".to_string());
    let id = record.header.identifier;

    // Parse authors from creator field
    let authors = if let Some(creator) = dc.creator {
        // zbMATH often has semicolon-separated authors
        creator
            .split(';')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    } else {
        vec![]
    };

    // Extract year from date field
    let year = if let Some(date_str) = dc.date {
        // Try to parse year from various date formats
        if let Ok(year_match) = date_str.chars().take(4).collect::<String>().parse::<u32>() {
            year_match
        } else {
            return Ok(None); // Skip records without valid year
        }
    } else {
        return Ok(None); // Skip records without date
    };

    // Extract venue from source field
    let venue = dc.source;

    Ok(Some(PublicationRecord {
        id,
        title,
        authors,
        year,
        venue,
        source: "zbmath".to_string(),
    }))
}
