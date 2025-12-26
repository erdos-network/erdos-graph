//! Orchestrates the scraping process across multiple publication sources.
//!
//! This module coordinates scraping from ArXiv, DBLP, and zbMATH, managing
//! checkpointing, chunking large date ranges, and ingesting results into the database.

use crate::config::load_config;
use crate::db::ingestion::{PublicationRecord, get_checkpoint, ingest_publication, set_checkpoint};
use crate::thread_safe_queue::{QueueConfig, ThreadSafeQueue};
use chrono::{DateTime, Duration, Utc};
use indradb::{Database, Datastore};
use std::collections::HashMap;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::{self, Instant};

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
    // Load configuration
    let config = load_config()?;

    // Determine which sources to scrape
    let sources: Vec<String> = if let Some(src) = source {
        vec![src.to_string()]
    } else {
        config.scrapers.enabled.clone()
    };

    // Define queue
    let queue = ThreadSafeQueue::new(QueueConfig::default());

    // Define heartbeat tracking
    let heartbeat_times = Arc::new(Mutex::new(HashMap::new()));
    let heartbeat_timeout = time::Duration::from_secs(config.heartbeat_timeout_s);

    // Define vector of sources to process
    let mut child_handles = vec![];

    for src in &sources {
        let last_checkpoint = get_checkpoint(src.as_str())?;

        // Determine effective date range based on mode
        let effective_start = match mode {
            "initial" => start_date.unwrap_or(Utc::now() - Duration::days(365 * 10)),
            "weekly" => last_checkpoint.unwrap_or(Utc::now() - Duration::days(7)),
            _ => return Err("Invalid mode".into()),
        };
        let effective_end = end_date.unwrap_or(Utc::now());

        // Spawn child processes
        let exe = std::env::current_exe()?;
        let mut cmd = Command::new(exe);

        cmd.arg("scrape-child")
            .arg("--source")
            .arg(src)
            .arg("--start")
            .arg(effective_start.to_rfc3339())
            .arg("--end")
            .arg(effective_end.to_rfc3339())
            .arg("--chunk-size")
            .arg(chunk_size.num_days().to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let mut child = cmd.spawn()?;
        let stdout = child.stdout.take().ok_or("Failed to capture stdout")?;
        let stderr = child.stderr.take().ok_or("Failed to capture stderr")?;

        // Init heartbeat time
        {
            let mut hb_times = heartbeat_times.lock().await;
            hb_times.insert(src.clone(), Instant::now());
        }

        // Create producer task for each child process
        let producer = queue.create_producer();
        let src_clone = src.clone();

        let handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                if let Ok(record) = serde_json::from_str::<PublicationRecord>(&line) {
                    let _ = producer.submit(record);
                } else {
                    eprintln!("Failed to parse line from {}: {}", src_clone, line);
                }
            }
        });

        // Create hearbeat reader task
        let hb_times_clone = heartbeat_times.clone();
        let src_clone2 = src.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();

            while let Ok(Some(line)) = reader.next_line().await {
                if line.contains("HEARTBEAT") {
                    let mut hb_times = hb_times_clone.lock().await;
                    hb_times.insert(src_clone2.clone(), Instant::now());
                }
            }
        });

        child_handles.push((child, handle, heartbeat_handle, src.clone(), effective_end));
    }

    // Consumer loop
    loop {
        if let Some(record) = queue.dequeue() {
            ingest_publication(datastore, record)?;
        } else {
            if queue.producers_finished() {
                break;
            }

            // Check for heartbeat timeouts
            let hb_times = heartbeat_times.lock().await;
            let now = Instant::now();
            for (src, last_heartbeat) in hb_times.iter() {
                if now.duration_since(*last_heartbeat) > heartbeat_timeout {
                    eprintln!(
                        "WARNING: Source {} has not sent a heartbeat in {:?}",
                        src, heartbeat_timeout
                    );
                }
            }
            drop(hb_times);

            tokio::time::sleep(tokio::time::Duration::from_millis(
                config.polling_interval_ms,
            ))
            .await;
        }
    }

    // Cleanup
    for (mut child, handle, heartbeat_handle, src, end_date) in child_handles {
        let _ = handle.await;
        let _ = heartbeat_handle.await;

        if child.wait().await?.success() {
            set_checkpoint(&src, end_date)?;
        }
    }

    Ok(())
}
