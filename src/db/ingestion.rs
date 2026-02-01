//! Publication ingestion orchestration and checkpointing logic.
//!
//! This module handles orchestrating the scraping process, including date range calculation,
//! chunking, and checkpoint management.

use crate::logger;
use chrono::{DateTime, Duration, Utc};
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use std::fs;
use std::path::Path;
use std::sync::Arc;

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
/// * `base_path` - The directory where checkpoints are stored
///
/// # Returns
/// `Some(DateTime)` if a checkpoint exists, `None` if this is the first scrape
pub fn get_checkpoint(
    source: &str,
    base_path: &Path,
) -> Result<Option<DateTime<Utc>>, Box<dyn std::error::Error>> {
    let path = base_path.join(format!("{}.txt", source));
    if path.exists() {
        let content = fs::read_to_string(&path)?;
        logger::debug(&format!("Read checkpoint for {}: {}", source, content));
        Ok(Some(
            DateTime::parse_from_rfc3339(&content)?.with_timezone(&Utc),
        ))
    } else {
        logger::debug(&format!("No checkpoint found for {}", source));
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
/// * `base_path` - The directory where checkpoints are stored
pub fn set_checkpoint(
    source: &str,
    date: DateTime<Utc>,
    base_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(base_path)?;
    fs::write(base_path.join(format!("{}.txt", source)), date.to_rfc3339())?;
    logger::info(&format!(
        "Updated checkpoint for {} to {}",
        source,
        date.to_rfc3339()
    ));
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
/// * `sources` - List of source identifiers to scrape (e.g., ["arxiv", "dblp", "zbmath"])
/// * `datastore` - Arc-wrapped HelixGraphEngine
/// * `config` - Configuration containing ingestion settings
///
/// # Returns
/// `Ok(())` on success, or an error if processing fails
#[coverage(off)]
pub async fn orchestrate_scraping_and_ingestion(
    mode: &str,
    sources: Vec<String>,
    datastore: Arc<HelixGraphEngine>,
    config: &crate::config::Config,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::scrapers::scraping_orchestrator::run_scrape;

    logger::info(&format!(
        "Orchestrating scraping for sources: {:?} in mode: {}",
        sources, mode
    ));

    let default_checkpoint_dir = "checkpoints".to_string();
    let checkpoint_dir = config
        .ingestion
        .checkpoint_dir
        .as_ref()
        .unwrap_or(&default_checkpoint_dir);
    let checkpoint_path = Path::new(checkpoint_dir);

    // Calculate start and end dates based on mode (source-agnostic, uses earliest checkpoint)
    let (start_date, end_date) =
        calculate_date_range(mode, &sources, &config.ingestion, checkpoint_path)?;

    logger::info(&format!(
        "Calculated date range: {} to {}",
        start_date, end_date
    ));

    // Break date range into chunks
    let chunks = chunk_date_range(start_date, end_date, config.ingestion.chunk_size_days)?;
    let total_chunks = chunks.len();
    logger::info(&format!(
        "Split date range into {} chunks of {} days",
        total_chunks, config.ingestion.chunk_size_days
    ));

    // Run run_scrape for each chunk (all sources in parallel) and update checkpoints
    for (i, (chunk_start, chunk_end)) in chunks.into_iter().enumerate() {
        logger::info(&format!(
            "Processing chunk {}/{}: {} to {}",
            i + 1,
            total_chunks,
            chunk_start,
            chunk_end
        ));

        // Pass sources list to run_scrape
        run_scrape(
            chunk_start,
            chunk_end,
            sources.clone(),
            datastore.clone(),
            config,
        )
        .await?;

        // Update checkpoints for all sources that were processed
        for src in &sources {
            set_checkpoint(src, chunk_end, checkpoint_path)?;
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
    base_path: &Path,
) -> Result<(DateTime<Utc>, DateTime<Utc>), Box<dyn std::error::Error>> {
    let now = Utc::now();

    // Find the earliest checkpoint across all sources
    let mut earliest_checkpoint: Option<DateTime<Utc>> = None;
    for source in sources {
        if let Some(checkpoint) = get_checkpoint(source, base_path)? {
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
pub(crate) fn chunk_date_range(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    chunk_size_days: u64,
) -> Result<DateRangeChunks, Box<dyn std::error::Error>> {
    if start >= end {
        return Err("Start date must be before end date".into());
    }

    let mut chunks = Vec::new();
    // Safety check: if chunk_size_days is 0, treat it as a single chunk for the entire range
    if chunk_size_days == 0 {
        chunks.push((start, end));
        return Ok(chunks);
    }

    let chunk_duration = Duration::days(chunk_size_days as i64);
    let mut current_start = start;

    while current_start < end {
        let chunk_end = std::cmp::min(current_start + chunk_duration, end);
        chunks.push((current_start, chunk_end));
        current_start = chunk_end;
    }

    Ok(chunks)
}
