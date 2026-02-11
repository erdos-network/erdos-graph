#![coverage(off)]
//! Benchmark scraping job
//!
//! This module provides a job to benchmark the scraping process for a specified number of weeks.

use crate::config::Config;
use crate::db::ingestion::orchestrate_scraping_and_ingestion;
use crate::logger;
use helix_db::helix_engine::traversal_core::HelixGraphEngine;
use std::sync::Arc;
use std::time::Instant;

/// Runs a benchmark scrape for a specified duration (in weeks).
///
/// This job modifies the configuration to scrape back `num_weeks` and times the execution.
/// It acquires the datastore lock to ensure exclusive access.
/// It uses a temporary checkpoint directory to ignore existing checkpoints and force
/// scraping of the requested duration.
///
/// # Arguments
/// * `num_weeks` - Number of weeks to scrape back
/// * `config` - Base application configuration
/// * `datastore` - Arc-wrapped HelixGraphEngine
///
/// # Returns
/// `Ok(Duration)` containing the elapsed time, or an error if execution fails
pub async fn run_benchmark(
    num_weeks: u64,
    mut config: Config,
    datastore: Arc<HelixGraphEngine>,
) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    let sources = config.scrapers.enabled.clone();
    logger::info(&format!(
        "Starting benchmark scrape for {} weeks on sources: {:?}",
        num_weeks, sources
    ));

    // Use subdirectories of configured paths for benchmark artifacts to ensure isolation and respect gitignore
    let base_checkpoint_dir = config
        .ingestion
        .checkpoint_dir
        .as_deref()
        .unwrap_or("checkpoints");
    let benchmark_checkpoint_dir = std::path::Path::new(base_checkpoint_dir).join("benchmark");

    let base_cache_dir = &config.scrapers.dblp.cache_dir;
    let benchmark_cache_dir = std::path::Path::new(base_cache_dir).join("benchmark");

    // Clean up existing benchmark directories to ensure a fresh run
    if benchmark_checkpoint_dir.exists() {
        std::fs::remove_dir_all(&benchmark_checkpoint_dir)?;
    }
    // We also clean the cache to benchmark the full scraping process including network requests
    if benchmark_cache_dir.exists() {
        std::fs::remove_dir_all(&benchmark_cache_dir)?;
    }

    config.ingestion.checkpoint_dir = Some(benchmark_checkpoint_dir.to_string_lossy().to_string());
    config.scrapers.dblp.cache_dir = benchmark_cache_dir.to_string_lossy().to_string();

    // Override weekly_days in config for the benchmark
    config.ingestion.weekly_days = num_weeks * 7;

    // Use XML mode for DBLP in benchmark (faster for large date ranges)
    let mut source_modes = std::collections::HashMap::new();
    if sources.contains(&"dblp".to_string()) {
        source_modes.insert("dblp".to_string(), "xml".to_string());
    }

    let start_time = Instant::now();

    match orchestrate_scraping_and_ingestion("weekly", sources.clone(), Some(source_modes), datastore, &config).await {
        Ok(_) => {
            let duration = start_time.elapsed();
            logger::info(&format!(
                "Benchmark completed successfully. Elapsed time: {:.2} seconds",
                duration.as_secs_f64()
            ));
            Ok(duration)
        }
        Err(e) => {
            logger::error(&format!("Benchmark failed: {}", e));
            Err(e)
        }
    }
}
