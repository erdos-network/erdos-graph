//! Publication scraping modules for various academic databases.
//!
//! This module provides scrapers for three major academic publication sources:
//! - ArXiv: Pre-print repository for physics, mathematics, and computer science
//! - DBLP: Computer science bibliography database
//! - zbMATH: Mathematics bibliography database
//!
//! Each scraper implements the `Scraper` trait that fetches publications
//! within a specified date range and returns them as `PublicationRecord` objects.

pub mod arxiv;
pub mod dblp;
pub mod scraper;
pub mod scraping_orchestrator;
pub mod zbmath;

// Re-export the trait and main scraper types
pub use arxiv::ArxivScraper;
pub use dblp::DblpScraper;
pub use scraper::Scraper;
pub use zbmath::ZbmathScraper;

#[cfg(test)]
pub mod tests;
