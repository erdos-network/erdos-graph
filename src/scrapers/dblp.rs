use crate::db::ingestion::PublicationRecord;
use chrono::{DateTime, Datelike, Utc};
use flate2::read::GzDecoder;
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::Client;
use std::io::{BufRead, BufReader, Cursor};

pub async fn scrape_range(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<PublicationRecord>, Box<dyn std::error::Error>> {
    let start_year = start_date.year();
    let end_year = end_date.year();

    let client = Client::new();
    let url = "https://dblp.org/xml/dblp.xml.gz";

    // Download the DBLP XML dump
    let response = client.get(url).send().await?;
    let body = response.bytes().await?;

    // Decompress the gzip file - Use BufReader to satisfy BufRead requirements
    let cursor = Cursor::new(body.to_vec());
    let decoder = GzDecoder::new(cursor);
    let buf_reader = BufReader::new(decoder);
    let mut reader = Reader::from_reader(buf_reader);
    reader.trim_text(true);

    let mut records = Vec::new();
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(ref e) => {
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;

                // Process article and inproceedings tags
                if tag_name == "article" || tag_name == "inproceedings" {
                    // Extract key attribute to get the DBLP key
                    let mut dblp_key = String::new();
                    for attr in e.attributes() {
                        let attr = attr?;
                        if std::str::from_utf8(attr.key.as_ref())? == "key" {
                            dblp_key = String::from_utf8(attr.value.to_vec())?;
                            break;
                        }
                    }

                    // Parse the element content - FIX: Properly handle the Result
                    match parse_dblp_element(&mut reader, tag_name, &dblp_key) {
                        Ok(Some(record)) => {
                            // Filter by year range
                            if record.year >= start_year as u32 && record.year <= end_year as u32 {
                                records.push(record);
                            }
                        }
                        Ok(None) => {
                            // Skip incomplete records
                        }
                        Err(e) => {
                            eprintln!("Error parsing DBLP element: {}", e);
                            // Continue processing other elements
                        }
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }

        buf.clear();
    }

    Ok(records)
}

/// Parses a DBLP XML element (article or inproceedings) into a PublicationRecord
fn parse_dblp_element<R: BufRead>(
    reader: &mut Reader<R>,
    element_type: &str,
    key: &str,
) -> Result<Option<PublicationRecord>, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    let mut title = String::new();
    let mut authors = Vec::new();
    let mut year: Option<u32> = None;
    let mut venue: Option<String> = None;
    let mut depth = 1;

    // Read until we find the closing tag for this element
    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => {
                depth += 1;
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;
                match tag_name {
                    "author" => {
                        match read_text_content(reader, "author") {
                            Ok(author_name) => authors.push(author_name),
                            Err(_) => {} // Skip on error
                        }
                        depth -= 1; // read_text_content consumed the end tag
                    }
                    "title" => {
                        match read_text_content(reader, "title") {
                            Ok(title_text) => title = title_text,
                            Err(_) => {} // Skip on error
                        }
                        depth -= 1;
                    }
                    "year" => {
                        match read_text_content(reader, "year") {
                            Ok(year_text) => year = year_text.parse().ok(),
                            Err(_) => {} // Skip on error
                        }
                        depth -= 1;
                    }
                    "journal" if element_type == "article" => {
                        match read_text_content(reader, "journal") {
                            Ok(journal_name) => venue = Some(journal_name),
                            Err(_) => {} // Skip on error
                        }
                        depth -= 1;
                    }
                    "booktitle" if element_type == "inproceedings" => {
                        match read_text_content(reader, "booktitle") {
                            Ok(booktitle_name) => venue = Some(booktitle_name),
                            Err(_) => {} // Skip on error
                        }
                        depth -= 1;
                    }
                    _ => {
                        // Skip other elements by reading through them
                        skip_element(reader, tag_name)?;
                        depth -= 1;
                    }
                }
            }
            Event::End(e) => {
                depth -= 1;
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;
                if tag_name == element_type && depth == 0 {
                    break;
                }
            }
            Event::Eof => {
                return Err("Unexpected EOF while parsing DBLP element".into());
            }
            _ => {}
        }
        buf.clear();
    }

    // Validate required fields
    if title.trim().is_empty() || authors.is_empty() || year.is_none() {
        return Ok(None); // Skip incomplete records
    }

    Ok(Some(PublicationRecord {
        id: key.to_string(),
        title: title.trim().to_string(),
        authors: authors
            .into_iter()
            .map(|a: String| a.trim().to_string())
            .collect(),
        year: year.unwrap(),
        venue,
        source: "dblp".to_string(),
    }))
}

/// Reads text content from an XML element
fn read_text_content<R: BufRead>(
    reader: &mut Reader<R>,
    element_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    let mut content = String::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Text(e) => {
                content.push_str(&e.unescape()?);
            }
            Event::CData(e) => {
                // Handle CDATA sections - these don't need unescaping
                content.push_str(std::str::from_utf8(e.as_ref())?);
            }
            Event::End(e) => {
                let tag_name_bytes = e.name().as_ref().to_vec();
                let tag_name = std::str::from_utf8(&tag_name_bytes)?;
                if tag_name == element_name {
                    break;
                }
            }
            Event::Eof => {
                return Err(format!("Unexpected EOF while reading {}", element_name).into());
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(content)
}

/// Skips an XML element and all its children
fn skip_element<R: BufRead>(
    reader: &mut Reader<R>,
    element_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    let mut depth = 1;

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(_) => {
                depth += 1;
            }
            Event::End(e) => {
                depth -= 1;
                if depth == 0 {
                    let tag_name_bytes = e.name().as_ref().to_vec();
                    let tag_name = std::str::from_utf8(&tag_name_bytes)?;
                    if tag_name == element_name {
                        break;
                    }
                }
            }
            Event::Eof => {
                return Err(format!("Unexpected EOF while skipping {}", element_name).into());
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(())
}
