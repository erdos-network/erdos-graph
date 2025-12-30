//! Publication ingestion orchestration and checkpointing logic.
//!
//! This module handles orchestrating the scraping process, including date range calculation,
//! chunking, and checkpoint management.

use chrono::{DateTime, Duration, Utc};
use indradb::{Database, Datastore};
use std::fs;
use std::path::Path;

/// Type alias for date range chunks: a vector of (start, end) date tuples.
type DateRangeChunks = Vec<(DateTime<Utc>, DateTime<Utc>)>;

/// Represents a publication scraped from an external source.
///
/// This is the intermediate data structure used between scraping and database ingestion.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PublicationRecord {
    /// Source-specific identifier (e.g., "arxiv:2024.12345", DBLP key)
    pub id: String,
    /// Full title of the publication
    pub title: String,
    /// List of author names in order
    pub authors: Vec<String>,
    /// Year of publication
    pub year: u32,
    /// Optional venue (journal, conference, etc.)
    pub venue: Option<String>,
    /// Source database identifier ("arxiv", "dblp", or "zbmath")
    pub source: String,
}

/// Retrieves the last checkpoint date for a specific scraping source.
///
/// Checkpoints allow incremental scraping by tracking the last successfully
/// processed date for each source. They are stored in the `checkpoints/` directory.
///
/// # Arguments
/// * `source` - The source identifier ("arxiv", "dblp", or "zbmath")
///
/// # Returns
/// `Some(DateTime)` if a checkpoint exists, `None` if this is the first scrape
pub fn get_checkpoint(source: &str) -> Result<Option<DateTime<Utc>>, Box<dyn std::error::Error>> {
    let path = format!("checkpoints/{}.txt", source);
    if Path::new(&path).exists() {
        let content = fs::read_to_string(path)?;
        Ok(Some(
            DateTime::parse_from_rfc3339(&content)?.with_timezone(&Utc),
        ))
    } else {
        Ok(None)
    }
}

/// Updates the checkpoint date for a specific scraping source.
///
/// This should be called after successfully processing each chunk of data
/// to enable resumption in case of failure.
///
/// # Arguments
/// * `source` - The source identifier ("arxiv", "dblp", or "zbmath")
/// * `date` - The timestamp to save as the new checkpoint
pub fn set_checkpoint(source: &str, date: DateTime<Utc>) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all("checkpoints")?;
    fs::write(format!("checkpoints/{}.txt", source), date.to_rfc3339())?;
    Ok(())
}

/// Orchestrates the complete scraping and ingestion process with chunked processing and checkpointing.
///
/// This function:
/// 1. Calculates start and end dates based on mode using `calculate_date_range`
/// 2. Breaks the date range into chunks using `chunk_date_range`
/// 3. Calls `run_scrape` for each chunk
///
/// # Arguments
/// * `mode` - Processing mode: "initial" (scrape last 10 years), "weekly" (incremental updates),
///   or "full" (scrape everything from beginning)
/// * `source` - Optional specific source to scrape ("arxiv", "dblp", or "zbmath"), or None for all enabled sources
/// * `datastore` - Mutable reference to the IndraDB datastore
///
/// # Returns
/// `Ok(())` on success, or an error if processing fails
#[coverage(off)]
pub async fn orchestrate_scraping_and_ingestion(
    mode: &str,
    source: Option<&str>,
    datastore: &mut Database<impl Datastore>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::config::load_config;
    use crate::scrapers::scraping_orchestrator::run_scrape;

    // Load configuration
    let config = load_config()?;

    // Determine which sources to process
    let sources: Vec<String> = if let Some(src) = source {
        vec![src.to_string()]
    } else {
        config.scrapers.enabled.clone()
    };

    // Calculate start and end dates based on mode (source-agnostic, uses earliest checkpoint)
    // Use the sources we're actually processing for checkpoint calculation
    let (start_date, end_date) = calculate_date_range(mode, &sources, &config.ingestion)?;

    // Break date range into chunks
    let chunks = chunk_date_range(start_date, end_date, config.ingestion.chunk_size_days)?;

    // Run run_scrape for each chunk (all sources in parallel) and update checkpoints
    for (chunk_start, chunk_end) in chunks {
        // Pass sources list to run_scrape
        run_scrape(chunk_start, chunk_end, sources.clone(), datastore).await?;

        // Update checkpoints for all sources that were processed
        for src in &sources {
            set_checkpoint(src, chunk_end)?;
        }
    }

    Ok(())
}

/// Calculates the date range for scraping based on mode and existing checkpoints.
///
/// This function is source-agnostic and uses the earliest checkpoint across all sources,
/// or calculates based on mode if no checkpoints exist.
///
/// # Arguments
/// * `mode` - Processing mode: "initial", "weekly", or "full"
/// * `sources` - List of source identifiers to check for checkpoints
/// * `config` - Configuration containing time range settings
///
/// # Returns
/// A tuple of (start_date, end_date)
#[coverage(off)]
fn calculate_date_range(
    mode: &str,
    sources: &[String],
    config: &crate::config::IngestionConfig,
) -> Result<(DateTime<Utc>, DateTime<Utc>), Box<dyn std::error::Error>> {
    let now = Utc::now();

    // Find the earliest checkpoint across all sources
    let mut earliest_checkpoint: Option<DateTime<Utc>> = None;
    for source in sources {
        if let Some(checkpoint) = get_checkpoint(source)? {
            earliest_checkpoint = match earliest_checkpoint {
                Some(earliest) => Some(std::cmp::min(earliest, checkpoint)),
                None => Some(checkpoint),
            };
        }
    }

    let start_date = match mode {
        "initial" => {
            if let Some(checkpoint) = earliest_checkpoint {
                checkpoint
            } else {
                // Use the configured start date (Erdos' first known publication, January 1932)
                DateTime::parse_from_rfc3339(&config.initial_start_date)?.with_timezone(&Utc)
            }
        }
        "weekly" => {
            if let Some(checkpoint) = earliest_checkpoint {
                checkpoint
            } else {
                now - Duration::days(config.weekly_days as i64)
            }
        }
        "full" => {
            if let Some(checkpoint) = earliest_checkpoint {
                checkpoint
            } else {
                // Start from a very early date (e.g., 1900)
                DateTime::parse_from_rfc3339("1900-01-01T00:00:00Z")?.with_timezone(&Utc)
            }
        }
        _ => return Err(format!("Invalid mode: {}", mode).into()),
    };

    Ok((start_date, now))
}

/// Breaks a date range into smaller time-based chunks.
///
/// # Arguments
/// * `start` - Start date of the range
/// * `end` - End date of the range
/// * `chunk_size_days` - Size of each chunk in days
///
/// # Returns
/// A vector of (chunk_start, chunk_end) tuples
#[coverage(off)]
fn chunk_date_range(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    chunk_size_days: u64,
) -> Result<DateRangeChunks, Box<dyn std::error::Error>> {
    if start >= end {
        return Err("Start date must be before end date".into());
    }

    let mut chunks = Vec::new();
    let chunk_duration = Duration::days(chunk_size_days as i64);
    let mut current_start = start;

    while current_start < end {
        let chunk_end = std::cmp::min(current_start + chunk_duration, end);
        chunks.push((current_start, chunk_end));
        current_start = chunk_end;
    }

    Ok(chunks)
}
