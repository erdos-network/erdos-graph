#![coverage(off)]
//! Weekly scraping scheduler
//!
//! This module sets up a cron-based scheduler that runs weekly scrapes for all enabled sources.

use crate::config::Config;
use crate::db::ingestion::orchestrate_scraping_and_ingestion;
use indradb::{Database, Datastore};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};

/// Starts the weekly scraping scheduler.
///
/// This function creates a cron job that runs every week to scrape new publications
/// from all enabled sources defined in the configuration.
///
/// # Arguments
/// * `config` - Configuration containing enabled sources and ingestion settings
/// * `datastore` - Arc-wrapped Mutex of the IndraDB datastore
///
/// # Returns
/// `Ok(JobScheduler)` on success, which must be kept alive for the scheduler to run
///
/// # Example
/// ```no_run
/// # use erdos_graph::config::Config;
/// # use erdos_graph::schedulers::scrape_weekly_update::start_weekly_scraper;
/// # use indradb::Database;
/// # use std::sync::Arc;
/// # use tokio::sync::Mutex;
/// #
/// # async fn example<D: indradb::Datastore + Send + 'static>(
/// #     config: Config,
/// #     datastore: Arc<Mutex<Database<D>>>,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// let scheduler = start_weekly_scraper(config, datastore).await?;
/// scheduler.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn start_weekly_scraper<D: Datastore + Send + 'static>(
    config: Config,
    datastore: Arc<Mutex<Database<D>>>,
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

            let mut db = datastore.lock().await;

            match orchestrate_scraping_and_ingestion("weekly", sources.clone(), &mut *db, &config)
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
