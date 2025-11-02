use crate::db::ingestion::PublicationRecord;
use chrono::{DateTime, Utc, Datelike};
use quick_xml::Reader;
use quick_xml::events::Event;
use std::io::Read;
use reqwest;
use flate2::read::GzDecoder;
use uuid;
use tokio;

/// Scrapes publication records from DBLP for a given date range.
///
/// # Implementation Notes
/// This function should:
/// - Download and parse the DBLP XML dump or use the DBLP API
/// - Filter publications by publication year within the specified range
/// - Parse XML entries into `PublicationRecord` structs
/// - Extract complete author lists, venue information, and publication years
/// - Handle DBLP's specific XML schema and data formats
///
/// # Arguments
/// * `start_date` - The beginning of the date range (inclusive)
/// * `end_date` - The end of the date range (exclusive)
///
/// # Returns
/// A vector of `PublicationRecord` objects representing publications from DBLP
///
/// # Example Future Usage
/// ```ignore
/// let start = Utc.ymd(2024, 1, 1).and_hms(0, 0, 0);
/// let end = Utc.ymd(2024, 12, 31).and_hms(23, 59, 59);
/// let records = scrape_range(start, end)?;
/// ```
#[coverage(off)]
pub fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    // Create a Tokio runtime to handle async calls
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(scrape_range_async(start_date, end_date))
}

#[coverage(off)]
async fn scrape_range_async(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    
    // Download DBLP XML dump
    let xml_content = download_dblp_xml().await?;
    
    // Parse XML and extract publications within date range
    parse_dblp_xml(&xml_content, start_date, end_date)
}

async fn download_dblp_xml() -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let response = client
        .get("https://dblp.org/xml/dblp.xml.gz")
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(format!("Failed to download DBLP XML: {}", response.status()).into());
    }
    
    let bytes = response.bytes().await?;
    
    // Decompress the gzipped content
    let mut decoder = GzDecoder::new(&bytes[..]);
    let mut xml_content = String::new();
    decoder.read_to_string(&mut xml_content)?;
    
    Ok(xml_content)
}

pub fn parse_dblp_xml(
    xml_content: &str,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let mut reader = Reader::from_str(xml_content);
    reader.trim_text(true);
    
    let mut records = Vec::new();
    let mut buf = Vec::new();
    let mut current_record: Option<PublicationRecord> = None;
    let mut current_text = String::new();
    let mut current_tag = String::new();
    
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                current_tag = tag_name.clone();
                
                match tag_name.as_str() {
                    "article" | "inproceedings" | "proceedings" | "book" | "incollection" | "phdthesis" | "mastersthesis" => {
                        current_record = Some(PublicationRecord {
                            id: uuid::Uuid::new_v4().to_string(),
                            title: String::new(),
                            authors: Vec::new(),
                            venue: None,
                            year: 0,
                            source: "dblp".to_string(),
                        });
                    }
                    _ => {}
                }
            }
            Ok(Event::Text(ref e)) => {
                current_text = e.unescape()?.to_string();
            }
            Ok(Event::End(ref e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                
                if let Some(ref mut record) = current_record {
                    match tag_name.as_str() {
                        "title" => record.title = current_text.clone(),
                        "author" => record.authors.push(current_text.clone()),
                        "journal" | "booktitle" => record.venue = Some(current_text.clone()),
                        "year" => {
                            if let Ok(year) = current_text.parse::<u32>() {
                                record.year = year;
                            }
                        }
                        "article" | "inproceedings" | "proceedings" | "book" | "incollection" | "phdthesis" | "mastersthesis" => {
                            // Check if the publication is within the date range
                            let pub_year = record.year as i32;
                            let start_year = start_date.year();
                            let end_year = end_date.year();
                            
                            if pub_year >= start_year && pub_year <= end_year && !record.title.is_empty() {
                                records.push(record.clone());
                            }
                            current_record = None;
                        }
                        _ => {}
                    }
                }
                current_text.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(format!("Error at position {}: {:?}", reader.buffer_position(), e).into()),
            _ => {}
        }
        buf.clear();
    }

    Ok(records)
}
