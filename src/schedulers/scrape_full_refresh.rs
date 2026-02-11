#![coverage(off)]
//! Full refresh scraping job
//!
//! This module provides a job to perform a full initial scrape for specified sources.

use crate::config::Config;
use crate::db::ingestion::orchestrate_scraping_and_ingestion;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use std::sync::Arc;

/// Runs a full refresh scrape for the specified sources.
///
/// This function acquires the datastore lock to ensure exclusive access
/// and runs the scraping pipeline in "initial" mode.
///
/// # Arguments
/// * `sources` - List of sources to scrape (e.g., "arxiv", "dblp")
/// * `config` - Application configuration
/// * `datastore` - Arc-wrapped HelixGraphEngine
///
/// # Returns
/// `Ok(())` on success, or an error if scraping fails
pub async fn run_full_refresh(
    sources: Vec<String>,
    config: Config,
    datastore: Arc<HelixGraphEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting full refresh scrape for sources: {:?}", sources);

    // Use XML mode for DBLP in full refresh (faster for large date ranges)
    let mut source_modes = std::collections::HashMap::new();
    if sources.contains(&"dblp".to_string()) {
        source_modes.insert("dblp".to_string(), "xml".to_string());
    }

    match orchestrate_scraping_and_ingestion("initial", sources.clone(), Some(source_modes), datastore, &config).await {
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
