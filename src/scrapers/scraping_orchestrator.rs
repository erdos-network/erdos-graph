//! Orchestrates the scraping process across multiple publication sources.
//!
//! This module coordinates scraping from ArXiv, DBLP, and zbMATH, managing
//! checkpointing, chunking large date ranges, and ingesting results into the database.

use crate::db::ingestion::{get_checkpoint, ingest_publication, set_checkpoint};
use crate::scrapers::{arxiv, dblp, zbmath};
use crate::utilities::generate_chunks;
use chrono::{DateTime, Duration, Utc};
use indradb::{Database, Datastore};

/// Orchestrates the complete scraping process for one or more publication sources.
///
/// This function handles:
/// - Mode selection (initial full scrape vs. incremental weekly updates)
/// - Checkpoint restoration to resume from last successful scrape
/// - Date range chunking to avoid memory issues with large datasets
/// - Sequential processing of chunks with checkpoint updates
///
/// # Arguments
/// * `mode` - Either "initial" (scrape last 10 years) or "weekly" (incremental updates)
/// * `start_date` - Optional override for the start date (defaults based on mode)
/// * `end_date` - Optional override for the end date (defaults to now)
/// * `chunk_size` - Duration of each scraping chunk (e.g., 7 days)
/// * `source` - Optional specific source to scrape ("arxiv", "dblp", or "zbmath"), or None for all
/// * `datastore` - Mutable reference to the IndraDB datastore
///
/// # Returns
/// `Ok(())` on success, or an error if any chunk fails
///
/// # Errors
/// - Invalid mode string
/// - Checkpoint file I/O errors
/// - Scraping errors from individual sources
/// - Database ingestion errors
#[coverage(off)]
pub async fn run_scrape(
    mode: &str,
    start_date: Option<DateTime<Utc>>,
    end_date: Option<DateTime<Utc>>,
    chunk_size: Duration,
    source: Option<&str>,
    datastore: &mut Database<impl Datastore>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Determine which sources to scrape (specific source or all)
    let sources = source
        .map(|s| vec![s])
        .unwrap_or_else(|| vec!["arxiv", "dblp", "zbmath"]);

    for src in sources {
        let last_checkpoint = get_checkpoint(src)?;

        // Determine effective date range based on mode
        let effective_start = match mode {
            "initial" => start_date.unwrap_or(Utc::now() - Duration::days(365 * 10)),
            "weekly" => last_checkpoint.unwrap_or(Utc::now() - Duration::days(7)),
            _ => return Err("Invalid mode".into()),
        };
        let effective_end = end_date.unwrap_or(Utc::now());

        // Break the date range into manageable chunks
        let chunks = generate_chunks(effective_start, effective_end, chunk_size);

        // Process each chunk and update checkpoint after success
        for (chunk_start, chunk_end) in chunks {
            run_chunk(src, chunk_start, chunk_end, datastore).await?;
            set_checkpoint(src, chunk_end)?;
        }
    }
    Ok(())
}

/// Processes a single date range chunk for a specific publication source.
///
/// This function:
/// - Calls the appropriate scraper based on source name
/// - Retrieves all publication records in the date range
/// - Ingests each record into the database via `ingest_publication`
///
/// # Arguments
/// * `source` - The source identifier ("arxiv", "dblp", or "zbmath")
/// * `start_date` - Beginning of the date range (inclusive)
/// * `end_date` - End of the date range (exclusive)
/// * `datastore` - Mutable reference to the IndraDB datastore
///
/// # Returns
/// `Ok(())` on success, or an error if scraping or ingestion fails
///
/// # Errors
/// - Unknown source name
/// - Scraping errors (network, parsing, API limits)
/// - Database ingestion errors
#[coverage(off)]
pub async fn run_chunk(
    source: &str,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    datastore: &mut Database<impl Datastore>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Dispatch to the appropriate scraper
    let records = match source {
        "arxiv" => arxiv::scrape_range(start_date, end_date)?,
        "dblp" => dblp::scrape_range(start_date, end_date)?,
        "zbmath" => zbmath::scrape_range(start_date, end_date).await?,
        _ => return Err("Unknown source".into()),
    };

    // Ingest all records into the database
    for record in records {
        ingest_publication(datastore, record)?;
    }
    Ok(())
}
