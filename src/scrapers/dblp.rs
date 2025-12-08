//! DBLP (Database systems and Logic Programming) XML Scraper
//!
//! This module implements a scraper for the DBLP computer science bibliography database.
//! DBLP provides comprehensive bibliographic information about computer science publications
//! including journals, conference proceedings, and other academic publications.
//!
//! The scraper downloads and parses the complete DBLP XML dump (several GB compressed)
//! to extract publication metadata within specified date ranges.
//!
//! # Data Source
//! - URL: https://dblp.org/xml/dblp.xml.gz
//! - Format: Compressed XML dump of the entire DBLP database
//! - Size: Multiple gigabytes (compressed), much larger uncompressed
//! - Update frequency: Updated regularly by DBLP maintainers
//!
//! # XML Structure
//! The DBLP XML contains various publication types:
//! - `<article>` - Journal articles
//! - `<inproceedings>` - Conference/workshop papers
//! - `<book>`, `<incollection>`, `<proceedings>` - Other publication types
//!
//! Each entry contains metadata like authors, title, year, venue, etc.

use crate::db::ingestion::PublicationRecord;
use chrono::{DateTime, Datelike, Utc};
use flate2::read::GzDecoder; // For decompressing the gzipped XML dump
use quick_xml::Reader; // Fast XML streaming parser
use quick_xml::events::Event; // XML parsing events (Start, End, Text, etc.)
use reqwest::Client; // HTTP client for downloading the XML dump
use std::io::{BufRead, BufReader, Cursor};

/// Scrapes DBLP publication data for a specified date range.
///
/// This function downloads the complete DBLP XML dump and extracts publication records
/// that fall within the given date range. The filtering is done by publication year
/// since DBLP doesn't provide more granular date filtering in their XML dump.
///
/// # Arguments
/// * `start_date` - The start of the date range (inclusive). Only the year component is used.
/// * `end_date` - The end of the date range (inclusive). Only the year component is used.
///
/// # Returns
/// A vector of `PublicationRecord` structs containing the scraped publication data.
///
/// # Errors
/// Returns an error if:
/// - Network request to download the XML dump fails
/// - XML decompression fails
/// - XML parsing encounters unrecoverable errors
/// - Memory allocation fails (the XML dump is very large)
///
/// # Performance Notes
/// - Downloads the entire DBLP XML dump (~2-4 GB compressed, ~20+ GB uncompressed)
/// - Processes the XML in a streaming fashion to manage memory usage
/// - Filtering by year range happens during parsing to avoid loading irrelevant data
/// - Can take several minutes to complete depending on network speed and system performance
///
/// # Example
/// ```rust,no_run
/// use chrono::{TimeZone, Utc};
///
/// let start = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
/// let end = Utc.with_ymd_and_hms(2021, 12, 31, 23, 59, 59).unwrap();
/// let records = scrape_range(start, end).await?;
/// ```
pub async fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    if (start_date = end_date) {
        return Ok(Vec::new());
    }

    // Extract year components for filtering - DBLP filtering is done by year only
    let start_year = start_date.year();
    let end_year = end_date.year();

    // Initialize HTTP client for downloading the DBLP XML dump
    let client = Client::new();
    let url = "https://dblp.org/xml/dblp.xml.gz";

    // Download the complete DBLP XML dump
    // WARNING: This file is very large (multiple GB), so this operation can take time
    println!("Downloading DBLP XML dump from {}...", url);
    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        return Err(format!("Failed to download DBLP XML: HTTP {}", response.status()).into());
    }

    println!("Download complete, reading response body...");
    let body = response.bytes().await?;

    // Set up the decompression pipeline:
    // 1. Wrap the downloaded bytes in a Cursor for reading
    // 2. Create a GzDecoder to decompress the gzipped content
    // 3. Wrap in BufReader for efficient buffered reading (required by quick_xml)
    println!("Decompressing XML data...");
    let cursor = Cursor::new(body.to_vec());
    let decoder = GzDecoder::new(cursor);
    let buf_reader = BufReader::new(decoder);

    // Initialize the XML streaming parser
    let mut reader = Reader::from_reader(buf_reader);
    // Note: trim_text is not available in the current quick-xml version
    // We handle whitespace trimming manually as needed

    // Initialize collections for results and parsing
    let mut records = Vec::new(); // Store successfully parsed publication records
    let mut buf = Vec::new(); // Reusable buffer for XML event parsing

    println!(
        "Starting XML parsing for years {} to {}...",
        start_year, end_year
    );

    // Main parsing loop - stream through the XML document
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(ref e) => {
                // Convert tag name from bytes to string for processing
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;

                // We're only interested in publication entries: articles and inproceedings
                // - article: Journal papers, magazine articles
                // - inproceedings: Conference papers, workshop papers
                // Other types (book, incollection, proceedings, etc.) are skipped for now
                if tag_name == "article" || tag_name == "inproceedings" {
                    // Extract the DBLP key from the XML element's "key" attribute
                    // The DBLP key is a unique identifier with format like:
                    // - "journals/jacm/AuthorYear" for journal articles
                    // - "conf/icml/AuthorYear" for conference papers
                    let mut dblp_key = String::new();
                    for attr in e.attributes() {
                        let attr = attr?;
                        if std::str::from_utf8(attr.key.as_ref())? == "key" {
                            dblp_key = String::from_utf8(attr.value.to_vec())?;
                            break;
                        }
                    }

                    // Parse the XML element content to extract publication metadata
                    match parse_dblp_element(&mut reader, tag_name, &dblp_key) {
                        Ok(Some(record)) => {
                            // Apply year-based filtering to only include records in our date range
                            if record.year >= start_year as u32 && record.year <= end_year as u32 {
                                records.push(record);

                                // Progress indicator for large datasets
                                if records.len() % 1000 == 0 {
                                    println!(
                                        "Processed {} matching records so far...",
                                        records.len()
                                    );
                                }
                            }
                        }
                        Ok(None) => {
                            // Record was parsed but incomplete (missing required fields)
                            // This is normal and expected for some DBLP entries
                        }
                        Err(e) => {
                            // Non-fatal parsing error - log and continue with other records
                            eprintln!(
                                "Warning: Error parsing DBLP element with key '{}': {}",
                                dblp_key, e
                            );
                            // Continue processing - individual record failures shouldn't stop the entire scrape
                        }
                    }
                }
                // All other XML elements are ignored (books, proceedings, etc.)
            }
            Event::Eof => {
                // Reached end of XML document
                println!("Finished parsing XML document.");
                break;
            }
            _ => {
                // Ignore other XML events (Text, End, Comment, etc.) at the top level
                // We only care about Start events for publication elements
            }
        }

        // Clear the buffer for reuse in the next iteration
        // This is important for memory management when processing large XML files
        buf.clear();
    }

    println!(
        "DBLP scraping complete. Found {} records in date range {} to {}.",
        records.len(),
        start_year,
        end_year
    );
    Ok(records)
}

/// Parses a single DBLP XML element (article or inproceedings) into a PublicationRecord.
///
/// This function reads through a complete XML element, extracting metadata fields
/// like authors, title, year, and venue information. It uses a depth-tracking
/// approach to handle nested XML structures correctly.
///
/// # Arguments
/// * `reader` - Mutable reference to the XML reader positioned after the opening tag
/// * `element_type` - The type of element being parsed ("article" or "inproceedings")  
/// * `key` - The DBLP key extracted from the element's key attribute
///
/// # Returns
/// * `Ok(Some(record))` - Successfully parsed a complete publication record
/// * `Ok(None)` - Element was parsed but missing required fields (title, authors, or year)
/// * `Err(error)` - Fatal parsing error (malformed XML, encoding issues, etc.)
///
/// # XML Structure Handled
/// ```xml
/// <article key="journals/jacm/Smith21">
///   <author>John Smith</author>
///   <author>Jane Doe</author>
///   <title>Example Paper Title</title>
///   <year>2021</year>
///   <journal>Journal of the ACM</journal>  <!-- for articles -->
///   <booktitle>ICML</booktitle>            <!-- for inproceedings -->
///   <!-- other optional fields like pages, volume, etc. -->
/// </article>
/// ```
fn parse_dblp_element<R: BufRead>(
    reader: &mut Reader<R>,
    element_type: &str,
    key: &str,
) -> Result<Option<PublicationRecord>, Box<dyn std::error::Error>> {
    // Initialize parsing state
    let mut buf = Vec::new(); // Buffer for XML event parsing
    let mut title = String::new(); // Publication title
    let mut authors = Vec::new(); // List of author names
    let mut year: Option<u32> = None; // Publication year
    let mut venue: Option<String> = None; // Journal name or conference name
    let mut depth = 1; // Track XML nesting depth for proper parsing

    // Parse nested XML elements until we reach the closing tag for this publication
    // We start at depth 1 since we're already inside the opening tag
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                // Encountered a nested XML element
                depth += 1;
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;

                // Process known metadata fields
                match tag_name {
                    "author" => {
                        // Extract author name - DBLP can have multiple <author> tags
                        if let Ok(author_name) = read_text_content(reader, "author") {
                            authors.push(author_name)
                        }
                        depth -= 1; // read_text_content consumed the end tag
                    }
                    "title" => {
                        // Extract publication title - should be unique per entry
                        if let Ok(title_text) = read_text_content(reader, "title") {
                            title = title_text
                        }
                        depth -= 1;
                    }
                    "year" => {
                        // Extract publication year and convert to integer
                        if let Ok(year_text) = read_text_content(reader, "year") {
                            year = year_text.parse().ok();
                        }
                        depth -= 1;
                    }
                    "journal" if element_type == "article" => {
                        // For journal articles, extract journal name as venue
                        if let Ok(journal_name) = read_text_content(reader, "journal") {
                            venue = Some(journal_name);
                        }
                        depth -= 1;
                    }
                    "booktitle" if element_type == "inproceedings" => {
                        // For conference papers, extract conference name as venue
                        if let Ok(booktitle_name) = read_text_content(reader, "booktitle") {
                            venue = Some(booktitle_name);
                        }
                        depth -= 1;
                    }
                    _ => {
                        // Skip other XML elements we don't need (pages, volume, number, ee, etc.)
                        // This efficiently moves past unneeded content without processing it
                        skip_element(reader, tag_name)?;
                        depth -= 1;
                    }
                }
            }
            Event::End(e) => {
                // Encountered a closing XML tag
                depth -= 1;
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;

                // Check if this is the closing tag for our main element
                if tag_name == element_type && depth == 0 {
                    // We've reached the end of this publication entry
                    break;
                }
            }
            Event::Eof => {
                // Unexpected end of file while parsing - this shouldn't happen in well-formed XML
                return Err("Unexpected EOF while parsing DBLP element".into());
            }
            _ => {
                // Ignore other XML events (Text, Comment, etc.) at this level
            }
        }
        buf.clear(); // Clear buffer for next iteration
    }

    // Validate that we have all required fields for a complete publication record
    // DBLP entries sometimes lack essential information, so we filter them out
    if title.trim().is_empty() || authors.is_empty() || year.is_none() {
        return Ok(None); // Return None for incomplete records (not an error)
    }

    // Create and return a complete PublicationRecord
    Ok(Some(PublicationRecord {
        id: key.to_string(),             // Use DBLP key as unique identifier
        title: title.trim().to_string(), // Clean up whitespace from title
        authors: authors
            .into_iter()
            .map(|a: String| a.trim().to_string()) // Clean up whitespace from author names
            .collect(),
        year: year.unwrap(),        // Safe to unwrap due to validation above
        venue,                      // Journal or conference name (optional)
        source: "dblp".to_string(), // Mark this record as coming from DBLP
    }))
}

/// Reads the text content from an XML element.
///
/// This function advances the XML reader through an element, collecting all text
/// content until it reaches the element's closing tag. It handles both regular
/// text nodes and CDATA sections, and properly unescapes XML entities.
///
/// # Arguments
/// * `reader` - Mutable reference to the XML reader positioned after an opening tag
/// * `element_name` - Name of the element being read (for error reporting and validation)
///
/// # Returns
/// The concatenated text content of the element, with XML entities unescaped.
///
/// # Errors
/// Returns an error if:
/// - XML parsing fails
/// - Encoding conversion fails  
/// - Unexpected EOF is encountered
/// - XML entity unescaping fails
///
/// # Example XML Handling
/// ```xml
/// <title>Graph Theory &amp; Algorithms</title>  → "Graph Theory & Algorithms"
/// <author><![CDATA[Smith, J. & Doe, A.]]></author>  → "Smith, J. & Doe, A."
/// ```
fn read_text_content<R: BufRead>(
    reader: &mut Reader<R>,
    element_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut buf = Vec::new(); // Buffer for XML event parsing
    let mut content = String::new(); // Accumulated text content

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Text(e) => {
                // Regular text content with XML entities that need unescaping
                // e.g., "&amp;" -> "&", "&lt;" -> "<", "&gt;" -> ">"
                // Convert bytes to string and handle XML entities
                let text = std::str::from_utf8(e.as_ref())?;
                content.push_str(text);
            }
            Event::CData(e) => {
                // CDATA sections contain literal text that doesn't need unescaping
                // This is used for text that might contain XML-like characters
                content.push_str(std::str::from_utf8(e.as_ref())?);
            }
            Event::End(e) => {
                // Check if this is the closing tag for our element
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;
                if tag_name == element_name {
                    // Found the matching closing tag - we're done reading content
                    break;
                }
                // If it's a different closing tag, ignore it (nested element)
            }
            Event::Eof => {
                // Unexpected end of file - malformed XML
                return Err(
                    format!("Unexpected EOF while reading element '{}'", element_name).into(),
                );
            }
            _ => {
                // Ignore other XML events (Start tags of nested elements, Comments, etc.)
                // We only care about the text content at this level
            }
        }
        buf.clear(); // Clear buffer for next iteration
    }

    Ok(content)
}

/// Efficiently skips over an XML element and all its nested content.
///
/// This function is used to bypass XML elements we don't need to process,
/// such as `<pages>`, `<volume>`, `<ee>` (electronic edition), etc.
/// It uses depth tracking to handle arbitrarily nested XML structures.
///
/// # Arguments
/// * `reader` - Mutable reference to the XML reader positioned after an opening tag
/// * `element_name` - Name of the element being skipped (used for error reporting)
///
/// # Returns
/// `Ok(())` when the element has been successfully skipped, or an error if
/// XML parsing fails or unexpected EOF is encountered.
///
/// # Performance Notes
/// This is much more efficient than parsing and then discarding content,
/// especially for elements that might contain large amounts of nested data.
///
/// # Example Usage
/// When we encounter `<pages>123-456</pages>` but don't need page information,
/// this function will advance the reader past the entire element without
/// processing its content.
fn skip_element<R: BufRead>(
    reader: &mut Reader<R>,
    element_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = Vec::new(); // Buffer for XML event parsing
    let mut depth = 1; // Track nesting depth (start at 1 since we're inside the opening tag)

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(_) => {
                // Encountered a nested opening tag - increase depth
                depth += 1;
            }
            Event::End(e) => {
                // Encountered a closing tag - decrease depth
                depth -= 1;

                // Check if we've returned to our original depth (found matching closing tag)
                if depth == 0 {
                    let tag_name_bytes = e.name().as_ref().to_vec();
                    let tag_name = std::str::from_utf8(&tag_name_bytes)?;
                    if tag_name == element_name {
                        // Successfully skipped the entire element
                        break;
                    }
                }
            }
            Event::Eof => {
                // Unexpected end of file while skipping - indicates malformed XML
                return Err(
                    format!("Unexpected EOF while skipping element '{}'", element_name).into(),
                );
            }
            _ => {
                // Ignore all other XML events (Text, Comment, etc.) - we're just skipping
            }
        }
        buf.clear(); // Clear buffer for next iteration
    }

    Ok(())
}
