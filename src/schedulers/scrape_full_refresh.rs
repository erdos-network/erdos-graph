#![coverage(off)]
//! Full refresh scraping job
//!
//! This module provides a job to perform a full initial scrape for specified sources.

use crate::config::Config;
use crate::db::ingestion::orchestrate_scraping_and_ingestion;
use indradb::{Database, Datastore};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Runs a full refresh scrape for the specified sources.
///
/// This function acquires the datastore lock to ensure exclusive access
/// and runs the scraping pipeline in "initial" mode.
///
/// # Arguments
/// * `sources` - List of sources to scrape (e.g., "arxiv", "dblp")
/// * `config` - Application configuration
/// * `datastore` - Arc-wrapped Mutex of the IndraDB datastore
///
/// # Returns
/// `Ok(())` on success, or an error if scraping fails
pub async fn run_full_refresh<D: Datastore + Send + 'static>(
    sources: Vec<String>,
    config: Config,
    datastore: Arc<Mutex<Database<D>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting full refresh scrape for sources: {:?}", sources);

    // Acquire lock to ensure no other jobs are running
    let mut db = datastore.lock().await;

    match orchestrate_scraping_and_ingestion("initial", sources.clone(), &mut *db, &config).await {
        Ok(_) => {
            println!(
                "Full refresh scrape completed successfully for sources: {:?}",
                sources
            );
            Ok(())
        }
        Err(e) => {
            eprintln!("Full refresh scrape failed: {}", e);
            Err(e)
        }
    }
}
