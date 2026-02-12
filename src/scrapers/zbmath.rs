use crate::config::ZbmathSourceConfig;
use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::scraper::Scraper;
use crate::utilities::thread_safe_queue::QueueProducer;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::time::Duration as StdDuration;
use tokio::time::sleep;

/// ZbMATH scraper that implements the Scraper trait.
#[derive(Clone, Debug)]
pub struct ZbmathScraper {
    pub config: ZbmathSourceConfig,
}

impl ZbmathScraper {
    /// Create a new ZbmathScraper with default configuration.
    pub fn new() -> Self {
        Self {
            config: ZbmathSourceConfig::default(),
        }
    }

    /// Create a new ZbmathScraper with custom configuration.
    pub fn with_config(config: ZbmathSourceConfig) -> Self {
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
        producer: QueueProducer<PublicationRecord>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        scrape_range_with_config(start, end, self.config.clone(), producer).await
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
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    scrape_range_with_config(
        start_date,
        end_date,
        ZbmathSourceConfig::default(),
        producer,
    )
    .await
}

/// Scrapes publication records from zbMATH with custom configuration.
pub async fn scrape_range_with_config(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    config: ZbmathSourceConfig,
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    scrape_chunk_with_config(&client, start_date, end_date, &config, producer).await
}

/// Scrapes a date range with custom configuration.
///
/// This function handles pagination for the given date range and processes
/// all pages of results. Date range chunking is handled by the ingestion layer.
pub async fn scrape_chunk_with_config(
    client: &reqwest::Client,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    config: &ZbmathSourceConfig,
    producer: QueueProducer<PublicationRecord>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut resumption_token: Option<String> = None;

    // Pre-format dates outside the loop to avoid repeated allocations
    let from_date = start_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let until_date = end_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    loop {
        let mut params = vec![("verb", "ListRecords"), ("metadataPrefix", "oai_dc")];

        if let Some(token) = &resumption_token {
            // Use resumption token for pagination
            params.push(("resumptionToken", token.as_str()));
        } else {
            // First request - use date range
            params.push(("from", from_date.as_str()));
            params.push(("until", until_date.as_str()));
        }

        logger::debug(&format!(
            "Fetching zbMATH URL: {} with params: {:?}",
            config.base_url, params
        ));

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
                            logger::debug(&format!(
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
                #[allow(clippy::collapsible_if)]
                if let Some(publication) = convert_to_publication_record(record)? {
                    if let Err(e) = producer.submit(publication) {
                        logger::error(&format!("Failed to submit record: {}", e));
                    }
                }
            }

            // Check for pagination - use take to avoid cloning
            resumption_token = list_records.resumption_token;
            if resumption_token.is_none() || resumption_token.as_ref().is_none_or(|t| t.is_empty())
            {
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

    Ok(())
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

    // Parse authors from creator field - optimized to minimize allocations
    let authors = if let Some(creator) = dc.creator {
        // zbMATH often has semicolon-separated authors
        // Use split iterator directly to avoid intermediate Vec allocation
        creator
            .split(';')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .collect()
    } else {
        vec![]
    };

    // Extract year from date field - optimized parsing
    let year = if let Some(date_str) = dc.date {
        let date_str = date_str.trim();
        if date_str.len() < 4 {
            return Ok(None);
        }
        // Parse first 4 characters as year - use slice instead of take+collect
        if let Ok(year_val) = date_str[0..4].parse::<u32>() {
            year_val
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
