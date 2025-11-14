use crate::db::ingestion::PublicationRecord;
use crate::utilities::generate_chunks;
use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use std::time::Duration as StdDuration;
use tokio::time::sleep;

#[cfg(not(test))]
const ZBMATH_BASE_URL: &str = "https://oai.zbmath.org/v1/";

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
/// This function chunks large date ranges into smaller pieces to avoid overloading
/// the zbMATH API. It handles pagination, rate limiting, and error responses.
///
/// # Implementation Details
/// - Chunks the date range into 7-day periods to keep API requests manageable
/// - Makes requests to the zbMATH Open OAI-PMH API (<https://oai.zbmath.org/v1/>)
/// - Handles pagination using resumption tokens (max 100 records per request)
/// - Includes a 1-second delay between requests to be respectful to the API
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
/// let end = Utc.with_ymd_and_hms(2024, 1, 31, 23, 59, 59).unwrap();
/// let records = scrape_range(start, end).await?;
/// println!("Found {} publications", records.len());
/// ```
/// Scrapes publication records from zbMATH for a given date range.
#[allow(unused_variables)]
pub async fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    #[cfg(test)] base_url: &str,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    #[cfg(not(test))]
    let base_url = ZBMATH_BASE_URL;

    // Use 7-day chunks to keep requests manageable
    let chunk_size = Duration::days(7);
    let chunks = generate_chunks(start_date, end_date, chunk_size);

    let mut all_records = Vec::new();
    let client = reqwest::Client::new();

    println!(
        "Scraping zbMATH from {} to {} in {} chunks",
        start_date.format("%Y-%m-%d"),
        end_date.format("%Y-%m-%d"),
        chunks.len()
    );

    for (i, (chunk_start, chunk_end)) in chunks.iter().enumerate() {
        println!(
            "Processing chunk {}/{}: {} to {}",
            i + 1,
            chunks.len(),
            chunk_start.format("%Y-%m-%d"),
            chunk_end.format("%Y-%m-%d")
        );

        // Scrape this chunk with pagination
        let chunk_records = scrape_chunk(
            &client,
            *chunk_start,
            *chunk_end,
            #[cfg(test)]
            base_url,
        )
        .await?;
        println!("Found {} records in chunk {}", chunk_records.len(), i + 1);

        all_records.extend(chunk_records);

        // Be respectful to the API - wait 1 second between chunks
        if i < chunks.len() - 1 {
            sleep(StdDuration::from_secs(1)).await;
        }
    }

    println!("Total records scraped: {}", all_records.len());
    Ok(all_records)
}

/// Scrapes a single date chunk, handling pagination with resumption tokens.
#[allow(unused_variables)]
pub(crate) async fn scrape_chunk(
    client: &reqwest::Client,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    #[cfg(test)] base_url: &str,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    #[cfg(not(test))]
    let base_url = ZBMATH_BASE_URL;
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

        let response = client
            .get(base_url)
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
                            println!(
                                "No records found for date range {} to {}",
                                start_date.format("%Y-%m-%d"),
                                end_date.format("%Y-%m-%d")
                            );
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
            sleep(StdDuration::from_millis(500)).await;
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
