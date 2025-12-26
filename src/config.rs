#![coverage(off)]

use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
pub struct Config {
    pub scrapers: ScraperConfig,
    pub heartbeat_timeout_s: u64,
    pub polling_interval_ms: u64,
}

#[derive(Deserialize)]
pub struct ScraperConfig {
    pub enabled: Vec<String>,
}

pub fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let content = fs::read_to_string("config.json")?;
    serde_json::from_str(&content).map_err(Into::into)
}
