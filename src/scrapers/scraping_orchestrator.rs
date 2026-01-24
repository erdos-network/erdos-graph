use crate::config::Config;
use crate::db::ingestion::PublicationRecord;
use crate::logger;
use crate::scrapers::ingestion_utils::{IngestionContext, ingest_batch};
use crate::scrapers::{ArxivScraper, DblpScraper, Scraper, ZbmathScraper};
use crate::utilities::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
use chrono::{DateTime, Utc};
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use std::sync::Arc;
use std::time::Duration as StdDuration; // Added for timeout logic
use std::time::Instant;

/// Orchestrates the complete scraping process for one or more publication sources.
///
/// This function handles:
/// - Spawning tasks to scrape from specified sources
/// - Collecting scraped publication records via a thread-safe queue
/// - Ingesting records into the database
///
/// # Arguments
/// * `start_date` - Start date for scraping
/// * `end_date` - End date for scraping
/// * `sources` - List of sources to scrape ("arxiv", "dblp", or "zbmath")
/// * `datastore` - Arc-wrapped HelixGraphEngine
/// * `config` - Reference to the application configuration
///
/// # Returns
/// `Ok(())` on success, or an error if scraping or ingestion fails
///
/// # Errors
/// - Configuration loading errors
/// - Scraping errors from individual sources
/// - Database ingestion errors
#[coverage(off)]
pub async fn run_scrape(
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    sources: Vec<String>,
    datastore: Arc<HelixGraphEngine>,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    // Define queue
    let queue = ThreadSafeQueue::new(QueueConfig::default());

    logger::info(&format!(
        "Starting scraping for range {} to {}",
        start_date, end_date
    ));

    // Define vector of tasks to process
    let mut tasks = vec![];

    for src in &sources {
        logger::info(&format!("Spawning scraper task for {}", src));
        // Create producer task for each source
        let producer = queue.create_producer();
        let src_name = src.clone();
        let start = start_date;
        let end = end_date;
        let scraper_config = config.scrapers.clone();

        let task = tokio::spawn(async move {
            let scraper: Box<dyn Scraper> = match src_name.as_str() {
                "arxiv" => Box::new(ArxivScraper::with_config(scraper_config.arxiv)),
                "dblp" => Box::new(DblpScraper::with_config(scraper_config.dblp)),
                "zbmath" => Box::new(ZbmathScraper::new()), // Zbmath not updated yet
                _ => {
                    logger::error(&format!("Unknown source: {}", src_name));
                    return;
                }
            };

            logger::info(&format!("{} scraper started", src_name));
            match scraper.scrape_range(start, end, producer).await {
                Ok(()) => {
                    logger::info(&format!("{} scraper finished", src_name));
                }
                Err(e) => {
                    logger::error(&format!("Scraping failed for {}: {}", src_name, e));
                }
            }
        });

        tasks.push(task);
    }

    // Consumer loop
    let mut ingested_count = 0;
    let mut context = IngestionContext::new(config);

    // Preload authors and edges to speed up ingestion
    if let Err(e) = context.preload_authors(&datastore) {
        logger::error(&format!("Failed to preload authors: {}", e));
    }
    if let Err(e) = context.preload_edge_bloom(&datastore) {
        logger::error(&format!("Failed to preload edge bloom filter: {}", e));
    }

    let loop_start_time = Instant::now();
    let mut last_log_time = Instant::now();
    let mut last_log_count = 0;

    // Batching configuration
    const BATCH_SIZE: usize = 100;
    const BATCH_TIMEOUT: StdDuration = StdDuration::from_millis(200);
    let mut batch: Vec<PublicationRecord> = Vec::with_capacity(BATCH_SIZE);
    let mut last_batch_flush = Instant::now();

    loop {
        // Attempt to dequeue
        let record_opt = queue.dequeue();
        let is_none = record_opt.is_none();

        if let Some(record) = record_opt {
            batch.push(record);
        }

        let should_flush = batch.len() >= BATCH_SIZE
            || (!batch.is_empty() && last_batch_flush.elapsed() >= BATCH_TIMEOUT)
            || (is_none && queue.producers_finished() && !batch.is_empty());

        if should_flush {
            let batch_len = batch.len();
            if let Err(e) =
                ingest_batch(std::mem::take(&mut batch), datastore.clone(), config, &mut context).await
            {
                logger::error(&format!("Failed to ingest batch: {}", e));
            }
            ingested_count += batch_len;
            last_batch_flush = Instant::now();

            if ingested_count - last_log_count >= 100 {
                let now = Instant::now();
                let elapsed = now.duration_since(last_log_time).as_secs_f64();
                let total_elapsed = now.duration_since(loop_start_time).as_secs_f64();

                let current_rate = (ingested_count - last_log_count) as f64 / elapsed;
                let overall_rate = ingested_count as f64 / total_elapsed;

                logger::info(&format!(
                    "Ingested {} records (Current: {:.2} rec/s, Overall: {:.2} rec/s)",
                    ingested_count, current_rate, overall_rate
                ));

                last_log_time = now;
                last_log_count = ingested_count;
            }
        }

        if is_none {
            if queue.producers_finished() && batch.is_empty() {
                break;
            }
            // Sleep only if we didn't just flush (to avoid busy loop during batch accumulation if queue is slow)
            if !should_flush {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Reduced sleep for responsiveness
            }
        }
    }

    // Cleanup
    for task in tasks {
        let _ = task.await;
    }

    logger::info("Scraping orchestration finished for current chunk");

    Ok(())
}
