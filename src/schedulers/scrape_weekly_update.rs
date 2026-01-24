#![coverage(off)]
//! Weekly scraping scheduler
//!
//! This module sets up a cron-based scheduler that runs weekly scrapes for all enabled sources.

use crate::config::Config;
use crate::db::ingestion::orchestrate_scraping_and_ingestion;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use std::sync::Arc;
use tokio_cron_scheduler::{Job, JobScheduler};

/// Starts the weekly scraping scheduler.
///
/// This function creates a cron job that runs every week to scrape new publications
/// from all enabled sources defined in the configuration.
///
/// # Arguments
/// * `config` - Configuration containing enabled sources and ingestion settings
/// * `datastore` - Arc-wrapped HelixGraphEngine
///
/// # Returns
/// `Ok(JobScheduler)` on success, which must be kept alive for the scheduler to run
pub async fn start_weekly_scraper(
    config: Config,
    datastore: Arc<HelixGraphEngine>,
) -> Result<JobScheduler, Box<dyn std::error::Error>> {
    let scheduler = JobScheduler::new().await?;

    let config_clone = config.clone();
    let sources = config.scrapers.enabled.clone();

    // Create a weekly job (runs every Sunday at 2 AM)
    let job = Job::new_async("0 0 2 * * Sun", move |_uuid, _lock| {
        let sources = sources.clone();
        let datastore = datastore.clone();
        let config = config_clone.clone();

        Box::pin(async move {
            println!("Starting weekly scrape for sources: {:?}", sources);

            match orchestrate_scraping_and_ingestion("weekly", sources.clone(), datastore, &config)
                .await
            {
                Ok(_) => println!(
                    "Weekly scrape completed successfully for sources: {:?}",
                    sources
                ),
                Err(e) => eprintln!("Weekly scrape failed: {}", e),
            }
        })
    })?;

    scheduler.add(job).await?;

    Ok(scheduler)
}
