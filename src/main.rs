#![feature(coverage_attribute)]
#![coverage(off)]
use clap::{Parser, Subcommand};
use erdos_graph::config::load_config;
use erdos_graph::db::client::init_datastore;
use erdos_graph::logger::{self, AsyncLogger, init_logger};
use erdos_graph::schedulers::{scrape_benchmark, scrape_full_refresh, scrape_weekly_update};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Parser)]
#[command(name = "erdos-graph")]
#[command(about = "Erdos Graph Scraping & Ingestion Engine", long_about = None)]
struct Cli {
    /// Path to RocksDB database
    #[arg(long, default_value = "./graph.db")]
    db_path: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the weekly scheduler daemon
    Daemon,
    /// Run a full refresh scrape
    FullRefresh {
        /// Sources to scrape (e.g., "arxiv", "dblp")
        #[arg(short, long, value_delimiter = ',', num_args = 1.., required = true)]
        sources: Vec<String>,
    },
    /// Run a benchmark scrape
    Benchmark {
        /// Number of weeks to benchmark
        #[arg(short, long)]
        weeks: u64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    init_logger(AsyncLogger::new());

    let cli = Cli::parse();

    // Load config
    let config = load_config()?;

    // Initialize database
    let db_path = Path::new(&cli.db_path);
    let datastore = Arc::new(Mutex::new(init_datastore(db_path)?));

    match cli.command {
        Commands::Daemon => {
            logger::info("Starting weekly scheduler daemon...");
            let scheduler = scrape_weekly_update::start_weekly_scraper(config, datastore).await?;
            scheduler.start().await?;

            logger::info("Daemon running. Press Ctrl+C to stop.");
            tokio::signal::ctrl_c().await?;
            logger::info("Shutting down...");
        }
        Commands::FullRefresh { sources } => {
            scrape_full_refresh::run_full_refresh(sources, config, datastore).await?;
        }
        Commands::Benchmark { weeks } => {
            scrape_benchmark::run_benchmark(weeks, config, datastore).await?;
        }
    }

    Ok(())
}
